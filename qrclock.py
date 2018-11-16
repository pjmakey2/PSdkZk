#!/usr/bin/env python
#coding:utf-8
# Author:  julian --<pjmakey2@gmail.com>
# Twiter:  https://twitter.com/pjmakey
# Purpose: Populated database with iclock data
# Created: 16/11/11
#  CREATE TABLE `attendance` (
#   `id` int(11) NOT NULL AUTO_INCREMENT,
#   `cedula` int(11) DEFAULT NULL,
#   `nombre` varchar(80) DEFAULT NULL,
#   `apellido` varchar(80) DEFAULT NULL,
#   `fecha` date DEFAULT NULL,
#   `hora` time DEFAULT NULL,
#   `clock` varchar(30) DEFAULT NULL,
#   `tauth` varchar(30) DEFAULT NULL,
#   `estado` varchar(20) DEFAULT NULL,
#   `empresa` varchar(60) DEFAULT NULL,
#   `horario` datetime DEFAULT NULL,
#   `team` varchar(80) DEFAULT 'ND',
#   `leader` tinyint(1) DEFAULT '0',
#   `bloqueo` tinyint(1) DEFAULT '0',
#   `motivo` varchar(80) DEFAULT 'ND',
#   `justificacion` varchar(200) DEFAULT 'ND',
#   `desbloqueo_por` varchar(120) DEFAULT NULL,
#   `desbloqueo_fecha` datetime DEFAULT NULL,
#   `afecto` tinyint(1) DEFAULT '1',
#   PRIMARY KEY (`id`)
# ) ENGINE=InnoDB AUTO_INCREMENT=1212512 DEFAULT CHARSET=latin1
import sys, logging
import MySQLdb

class QuerysClock(object):
    """
    Manage the insert data to the database
    """
    def __init__(self, host, user, passwd, db):
        """
        Connect to the database, and initialize the logging file
        """
        logging.basicConfig(format='%(asctime)s %(message)s',filename="c:\\iclock.log",level=logging.DEBUG)                        
        self._conn = MySQLdb.connect(host=host,
                                     user=user,
                                     passwd=passwd,
                                     db=db)
        
    def InsertData(self, data):
        """
        Insert data in the clock
        @param data.
        is a tuple 
        """
        cursor = self._conn.cursor()
        sqli = "insert into attendance (cedula, nombre, apellido, fecha, hora, clock, tauth, estado, empresa) values (%s, %s, %s, %s, %s, %s, %s, %s, %s )"
        #logging.info("Ejecutando Insercion %s" % sqli)
        #logging.info("datos %s,%s,%s,%s,%s,%s,%s, %s, %s" % data)
        try:
            cursor.execute(sqli, data)
            self._conn.commit()                        
            #logging.info("insertando valores en la base de datos %s, %s, %s, %s, %s, %s, %s, %s, %s" % data)
        except:
            self._conn.rollback()
            cursor.execute(sqli, data)
            self._conn.commit()            
            #logging.info("insertando valores en la base de datos %s, %s, %s, %s, %s, %s, %s, %s, %s" % data)
            
    def CheckFirstTime(self, data):
        """
        checks if the first time in the date of the data
        we manage this by the time and name of the user
        @param data must be a simple tuple
        """
        cursor = self._conn.cursor()
        sqli = """select * from attendance where cedula = %s and fecha = %s and hora = %s and estado = %s"""
        #logging.info("Ejecutando query %s" % sqli)
        #logging.info("datos %s,%s,%s, %s" % data)
        try:
            cursor.execute(sqli, data)
            self._conn.commit()
        except:
            self._conn.rollback()
            cursor.execute(sqli, data)
            self._conn.commit()
            
        result = cursor.fetchall()

        if result:
            #logging.info("el dato ya existe en la base de datos %s, %s, %s, %s" % data)
            return False
        else:
            #logging.info("primera instancia del dato en la base de datos %s, %s, %s, %s" % data)
            return True            
