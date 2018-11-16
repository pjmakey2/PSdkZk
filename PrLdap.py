#!/usr/bin/env python
#coding:utf-8
# Author:  julian --<pjmakey2@gmail.com>
# Twiter:  https://twitter.com/pjmakey
# Purpose: Proxy ldap
# Created: 22/12/11

import ldap

########################################################################
class ProxyLdap(object):
    """With this class we can control the input of the attendance clock"""

    #----------------------------------------------------------------------
    def __init__(self):
        """Constructor"""

        self.con = ldap.initialize("ldap://somehost")
        self.con.simple_bind_s("cn=somecnmanager", "notXXX")
        self.base_dn = "ou=users,dc=root,dc=org"

    #----------------------------------------------------------------------
    def FormatUserdataTin(self, data):
        """
        Format the estructur of the user by the given data (id).
        """
        result = self.con.search_s(self.base_dn, ldap.SCOPE_SUBTREE, '(NumeroDocumentoIdentidad=%s*)' % data, ["cn", "NumeroDocumentoIdentidad"])

        return result
    #----------------------------------------------------------------------
    def FormatUserCompany(self, data):
        """
        Return de company of the user, by the given cedula
        """
        result = self.con.search_s(self.base_dn, ldap.SCOPE_SUBTREE, '(NumeroDocumentoIdentidad=%s*)' % data, ["Empresa"])

        return result

    def closel(self):
        self.con.unbind()

if __name__=='__main__':
    unittest.main()
