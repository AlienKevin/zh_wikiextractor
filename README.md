# Stats

Number of articles: 1036759
Folder size: 1.26GB

# Increase URI size limit of Apache server

1. Open `/etc/apache2/apache2.conf` and insert:

```
LimitRequestLine 4294967296
LimitRequestFieldSize 4294967296
```

Raise the URI byte size limit to 4 GiB.

2. Open `/etc/apache2/sites-available/000-default.conf` and insert the same two lines:

```
LimitRequestLine 4294967296
LimitRequestFieldSize 4294967296
```

Reference: https://stackoverflow.com/a/57246448/6798201
