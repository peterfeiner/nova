<?xml version="1.0" encoding="UTF-8"?>
<server xmlns="http://docs.openstack.org/compute/api/v1.1"
        xmlns:atom="http://www.w3.org/2005/Atom"
        id="%(uuid)s"
        tenant_id="openstack" user_id="fake"
        name="%(name)s"
        host_id="%(hostid)s" progress="0"
        status="ACTIVE" admin_password="%(password)s"
        created="%(timestamp)s"
        updated="%(timestamp)s">
  <image id="%(uuid)s">
      <atom:link
          rel="bookmark"
          href="%(glance_host)s/images/%(uuid)s"/>
  </image>
  <flavor id="1">
      <atom:link
          rel="bookmark"
          href="%(host)s/flavors/1"/>
  </flavor>
  <metadata>
    <meta key="meta_var">meta_val</meta>
  </metadata>
  <addresses>
    <network id="private">
      <ip version="4" addr="%(ip)s" type="fixed" mac_addr="aa:bb:cc:dd:ee:ff"/>
    </network>
  </addresses>
  <atom:link
      rel="self"
      href="%(host)s/v3/servers/%(uuid)s"/>
  <atom:link
      rel="bookmark"
      href="%(host)s/servers/%(uuid)s"/>
</server>
