# vim: tabstop=4 shiftwidth=4 softtabstop=4
#
# Copyright 2012 OpenStack Foundation
# All Rights Reserved
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import threading

from keystoneclient.v2_0 import client as kclientv20
from neutronclient.common import exceptions
from neutronclient.v2_0 import client as nclientv20
from oslo.config import cfg

from nova.openstack.common.gettextutils import _
from nova.openstack.common import log as logging
from nova.openstack.common import timeutils

CONF = cfg.CONF
LOG = logging.getLogger(__name__)


def _get_client(token=None):
    params = {
        'endpoint_url': CONF.neutron_url,
        'timeout': CONF.neutron_url_timeout,
        'insecure': CONF.neutron_api_insecure,
        'ca_cert': CONF.neutron_ca_certificates_file,
    }

    if token:
        params['token'] = token
        params['auth_strategy'] = None
    else:
        # NOTE(pete5): If we get rid of _authenticate_with_neutronclient, this
        # else branch goes too.
        params['username'] = CONF.neutron_admin_username
        params['tenant_name'] = CONF.neutron_admin_tenant_name
        params['password'] = CONF.neutron_admin_password
        params['auth_url'] = CONF.neutron_admin_auth_url
        params['auth_strategy'] = CONF.neutron_auth_strategy
    return nclientv20.Client(**params)


def _create_keystone_client(**kwargs):
    return kclientv20.Client(username=CONF.neutron_admin_username,
                             tenant_name=CONF.neutron_admin_tenant_name,
                             password=CONF.neutron_admin_password,
                             auth_url=CONF.neutron_admin_auth_url,
                             insecure=CONF.neutron_api_insecure,
                             cacert=CONF.neutron_ca_certificates_file,
                             timeout=CONF.neutron_url_timeout)


class SharedAdminAuth(object):
    TOKEN_RENEW_WINDOW = 3600

    def __init__(self):
        self._lock = threading.Lock()
        self._token = None
        self._expires = None

    def _authenticate_with_neutronclient(self):
        client = _get_client()
        client.httpclient.authenticate()
        auth_ref = client.httpclient.service_catalog.get_token()
        self._token = auth_ref['id']
        self._expires = timeutils.parse_isotime(auth_ref['expires'])

    def _authenticate_with_keystoneclient(self):
        if CONF.neutron_auth_strategy != 'keystone':
            raise exceptions.Unauthorized(message=
                _('Unsupported auth strategy %s') % CONF.neutron_auth_strategy)

        client = _create_keystone_client(
            username=CONF.neutron_admin_username,
            tenant_name=CONF.neutron_admin_tenant_name,
            password=CONF.neutron_admin_password,
            auth_url=CONF.neutron_admin_auth_url,
            insecure=CONF.neutron_api_insecure,
            cacert=CONF.neutron_ca_certificates_file,
            timeout=CONF.neutron_url_timeout)

        auth_ref = client.auth_ref
        self._token = auth_ref.auth_token
        self._expires = auth_ref.expires

    def _authenticate(self):
        # NOTE(pete5): Should we authenticate with neutronclient or
        # keystoneclient?
        #
        # If we authenticate with keystoneclient, then we can
        # only support CONF.neutron_auth_strategy being 'keystone'. On the
        # other hand, if we use neutronclient, then we can defer authentication
        # to neutronclient. Knowing that neutronclient only supports keystone,
        # the difference between these two options may seem moot.  However, new
        # authentication strategies may be added to neutron in the future.  If
        # we go with neutronclient, we automatically get support for new
        # strategies as they're implemented in neutronclient. Unfortunately,
        # authenticating neutronclient is a bit of a hack since it makes use of
        # not-so-public APIs in neutronclient. Thus
        # self._authenticate_with_neutronclient might not work future
        # neutronclient releases.
        #
        # Another option would be to use self._authenticate_with_keystoneclient
        # if the authentication strategy is 'keystone' and fallback to private
        # neutron clients otherwise.
        #
        # I think it's overkill to add support for non-existent authentication
        # strategies, especially since new authentication strategies will
        # almost definitely only be added in a major OpenStack release, which
        # will require users to upgrade nova and neutron to use. So my
        # preference is to use keystoneclient. I've updated the unit tests
        # accordingly.
        #
        # I include both approaches code for the purpose of code review
        # and discussion. When the prefereed method is settled on, I'll elide
        # the rejected method and this long comment.

        #self._authenticate_with_neutronclient()
        self._authenticate_with_keystoneclient()

    def get_token(self):
        with self._lock:
            if self._token is None or\
               timeutils.is_soon(self._expires, self.TOKEN_RENEW_WINDOW):
                self._authenticate()
            return self._token

    def _reset(self):
        '''For use with unit tests.'''
        with self._lock:
            self._token = None
            self._expires = None

SHARED_ADMIN_AUTH = SharedAdminAuth()


def get_client(context, admin=False):
    # NOTE(dprince): In the case where no auth_token is present
    # we allow use of neutron admin tenant credentials if
    # it is an admin context.
    # This is to support some services (metadata API) where
    # an admin context is used without an auth token.
    if admin or (context.is_admin and not context.auth_token):
        return _get_client(SHARED_ADMIN_AUTH.get_token())

    # We got a user token that we can use that as-is
    if context.auth_token:
        token = context.auth_token
        return _get_client(token)

    # We did not get a user token and we should not be using
    # an admin token so log an error
    raise exceptions.Unauthorized()
