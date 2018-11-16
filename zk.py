#!/usr/bin/env python
#coding:utf-8
# Author:  julian --<pjmakey2@gmail.com>
# Twiter:  https://twitter.com/pjmakey
# Purpose: manage the dll of the zk software, thanks to sy, for the prototype code
# Created: 06/12/2011
# Todo
# 2012/01/06
# we add a ldap proxy to diferrent the user company
# 2018-01-29
# We change the way of store for a more convenient and historic way via pandas

# -*- coding: utf-8 -*-
from win32com.client import Dispatch
import sys
from datetime import datetime
import logging
import qrclock
import PrLdap
import pandas as pd
import dataset
logging.basicConfig(format='%(asctime)s %(message)s',filename="c:\\iclock.log",level=logging.DEBUG)

########################################################################
class ZkDispatch(object):
    """Manage the windows enviroment for the zk connections to the iclock 2000"""
    __doc__ = 'Manage the operations on the biometric clock'
    __userdata = []
    __logdata = []
    __clock = ""
    __today = datetime.today().strftime('%Y-%m-%d')
    __proxyldap = PrLdap.ProxyLdap()

    #----------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        """
        Instance the database class, and distpatch the windows dll of the zk sdk
        """

        try:
            self.dbmanag = qrclock.QuerysClock(host=kwargs.get('host'),
                                               user=kwargs.get('user'),
                                               passwd=kwargs.get('passwd'),
                                               db=kwargs.get('db'))
        except:
            logging.info('conexion fallida con la base de datos, no se realiza ninguna accion')
            sys.exit(1)
        try:
            self.zk = Dispatch("zkemkeeper.ZKEM")
        except:
            logging.info('falta instalar el sdk ZKEM')
            sys.exit(1)

    #----------------------------------------------------------------------
    def Connect(self, ip, port, mn):
        """
        first connections to the clock, and retrieve the initialize data
        Connect_Net(ip, port)
        """
        self.__clock = ip
        logging.info("conectando al reloj %s" % ip)

        #devPort = 4370
        #devIPAddr = ’192.168.30.198′

        flag = self.zk.Connect_Net(ip,port)
        if flag:
            logging.info("Conexion exitosa al reloj")
        else:
            logging.info("Error de conexion, verifique el dispositivo.. Saliendo del programa")
            sys.exit(1)

        if self.zk.ReadAllUserID(mn):
            #print self.zk.GetAllUserInfo(mn, 0, "", "", 0, False)

            while True:
                #s= self.zk.GetAllUserInfo(mn, 0, "", "", 0, False)
                s= self.zk.SSR_GetAllUserInfo(mn, 0, "", "", 0, False)
                #s= self.zk.GetAllUserID(mn, 0, None, 0, False)
                if s[0]:
                    #print type(s[3])
                    #print s
                    # logging.info(u'Leyendo datos de usuarios {} {}'.format(s[1], s[2]))
                    self.__userdata.append((s[1], s[2]))

                else:
                    break

    #----------------------------------------------------------------------
    def QueryAttendanceDb(self, dateb=__today, datee=__today, mn=1, **kwargs):
        """
        Read all the log data by the given date, .
        we use here the SSR_GetGeneralLogData function of the sdk that use the following parameters

        """
        tmpstore = []
        logging.info('Retrayendo informacion del reloj')
        if self.zk.ReadGeneralLogData(mn):
            while True:
                att = self.zk.SSR_GetGeneralLogData(mn, "", 0, 0,2018, 1, 29, 0, 0, 0, 0)
                #att = self.zk.SSR_GetGeneralLogData(mn, "", None, None, 2011, 5, 13, None, None, None, None)
                if att[0]:
                    tmpstore.append(att)
                else:
                    break
        datat = []
        for v in tmpstore:
            for u in self.__userdata:
                if v[1] == u[0]:
                    dtlog = datetime(v[4], v[5], v[6], v[7], v[8], v[9])
                    dlog = dtlog.strftime("%Y-%m-%d")
                    tlog = dtlog.strftime("%H:%M:%S")
                    ta = self.ChangeAuthType(v[2])
                    tc = self.ChangeCheckType(v[3])
                    user = u[1].replace(","," ")
                    # logging.info(u'Dato Formateado desde reloj {} {} {} {} {} {}'.format(v[1], user, ta, tc, dlog, tlog))
                    # self.__logdata.append((v[1], user, ta, tc, dlog, tlog))
                    datat.append({
                                 'cedula': v[1],
                                 'usuario': user,
                                 'modo_marcacion':  ta,
                                 'tipo_marcacion': tc,
                                 'fecha': dlog,
                                 'hora': tlog
                                 })
        now = datetime.now()

        dfatt = pd.DataFrame(datat)
        dfatt['timestramp'] = dfatt.apply(lambda x: '{} {}'.format(x.fecha, x.hora), axis=1)
        fname = 'c:\\fami\\clock\\historic\\clock_data_{}_{}.csv'.format(self.__clock.replace('.', ''), now.strftime('%Y%m%d%H%M%S'))
        dfatt.to_csv(fname, sep='|', quotechar='"', index=False, header=True, encoding='utf-8')
        logging.info('Habilitando Reloj')
        if kwargs.get('clear') == 'clear':
            self.ClearLog()
        self.Enabled(1, 1)
        logging.info('Armando dataframe y datos almacenados en {}'.format(fname))
        logging.info('Conectando a la base mediante dataset')
        dbmisc = dataset.connect('mysql://notXXX:notXXX@somehost/misc')
        dbrkfbits = dataset.connect('postgresql://notXXX:notXXX@somehost/bigdata')
        logging.info('Seleccionando tabla attendance')
        att = dbmisc['attendance']
        profiles = dbrkfbits['dashboard_userprofile']
        bulk_insert = []
        if kwargs.get('all') != 'all':
            dfatt['fecha'] = pd.to_datetime(dfatt['fecha'], format='%Y-%m-%d')
            logging.info('Retrayendo del DF datos del dia')
            dfatt = dfatt[dfatt['fecha'] == now.strftime('%Y-%m-%d')]
        for key, dp in dfatt.iterrows():
            if att.find_one(fecha=dp.fecha, hora=dp.hora, cedula=dp.cedula):
                continue
            r = self.__proxyldap.FormatUserCompany(dp.cedula)
            if r:
                empresa = r[0][1]["Empresa"][0].replace('abreviatura=x4,ou=empresas,dc=rkf,dc=org', 'Xempre4')\
                                                                     .replace('abreviatura=x1,ou=empresas,dc=rkf,dc=org', 'Xempre')\
                                                                     .replace('abreviatura=x2,ou=empresas,dc=rkf,dc=org', 'Xempre1')\
                                                                     .replace('abreviatura=x3,ou=empresas,dc=rkf,dc=org', 'Exmpre3')
            else:
                empresa = "NULL"
            if att.find_one(fecha=dp.fecha, hora=dp.hora, cedula=dp.cedula):
                continue
            nombre = dp.usuario.split(',')[0]
            apellido = dp.usuario.split(',')[-1]
            profileobj = profiles.find_one(cedula=dp.cedula)
            if profileobj:
                nombrefactura = profileobj['nombrefactura']
                try:
                    nombre = nombrefactura.split(',')[0].encode('utf-8', errors='replace')
                    apellido = nombrefactura.split(',')[1].encode('utf-8', errors='replace')
                except:
                    nombre = 'ND'
                    apellido = 'ND'
            bulk_insert.append(
                               dict(cedula=dp.cedula,
                                      nombre=nombre.strip(),
                                      apellido=apellido.strip(),
                                      fecha=dp.fecha,
                                      hora=dp.hora,
                                      clock=self.__clock,
                                      tauth=dp.modo_marcacion,
                                      estado=dp.tipo_marcacion,
                                      empresa=empresa,
                                    )
                               )
        logging.info('Realizando un bulk insert de los datos')
        att.insert_many(bulk_insert)
        self.__proxyldap.closel()
        logging.info("Datos guardados en la base de datos")
        logging.info("Cerrando conexion ldap")

    #----------------------------------------------------------------------
    def ClearLog(self, mn=1):
        """Clear all the log attendance of the lock"""
        logging.info("Borrando logs de registros")
        self.zk.ClearGLog(mn)
        logging.info("Borrando logs de registros Exito")

    #----------------------------------------------------------------------
    def Enabled(self, mn=1, state=1):
        """Enalbed or disable the device"""
        if state == 0:
            logging.info("Desactivando dispositivo")
        else:
            logging.info("Activando dispositivo")
        self.zk.EnableDevice(mn, state)


    #----------------------------------------------------------------------
    def ChangeCheckType(self, ctyp):
        """change the check type for a human data"""

        if ctyp == 0:
            return "Entrada"

        if ctyp == 1:
            return "Salida"

        if ctyp == 2:
            return "Salida Intermedia"

        if ctyp == 3:
            return "Entrada Intermedia"

        if ctyp == 4:
            return "Entrada Almuerzo"

        if ctyp == 5:
            return "Salida Almuerzo"

    #----------------------------------------------------------------------
    def ChangeAuthType(self, atyp):
        """change the auth type for a human data"""

        if atyp == 0:
            return "Clave"

        if atyp == 1:
            return "Huella"

        if atyp == 2:
            return "Tarjeta"

        if atyp == 3:
            return "Clave"

        if atyp == 4:
            return "Clave"

        if atyp == 5:
            return "Clave"

        if atyp == 6:
            return "Clave"

    #----------------------------------------------------------------------
    def CalculeTime(self, times):
        """
        calculate how long take the transactions
        """
        ini = times[0]
        end = times[1]
        horai, minutoi, segundoi = ini.split(":")
        horai = int(horai)
        minutoi = int(minutoi)
        segundoi = int(segundoi)

        valor0 = (horai*60)+(minutoi*60)+segundoi
        horae, minutoe, segundoe = end.split(":")
        horae = int(horae)
        minutoe = int(minutoe)
        segundoe = int(segundoe)
        valor1 = (horae*60)+(minutoe*60)+segundoe
        dur = valor1 - valor0
        logging.info("Duracion de la conexion %s segundos" % str(dur))


