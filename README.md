# Stats

Number of articles: 1036759
Folder size: 1.26GB

# Setup MediaWiki
1. Open `localhost:8080` in your favorite browser. Click on `complete the installation` to continue.

<img src="media/install-0-start.png" style="width:600px">

2. You will see the installation welcome page.

<img src="media/install-1-welcome-1.png" style="width:600px">

Scroll down and press `continue`.

<img src="media/install-1-welcome-2.png" style="width:600px">

3. Set `Wiki language` to `zh - 中文` and press `continue`.

<img src="media/install-2-language.png" style="width:600px">

4. Set `Database host` to be `database` and scroll down.

<img src="media/install-3-database-1.png" style="width:600px">

Set `Database username` to be `wikiuser` and `Database password` to be `example`. Then press `continue`.

<img src="media/install-3-database-2.png" style="width:600px">

5. Press `continue`

<img src="media/install-4-database-web.png" style="width:600px">

6. Set the `Name of wiki` to be `my_wiki`. Set the `Your username` to be anything you like and set the password to anything you like. Then scroll down.

<img src="media/install-5-name-1.png" style="width:600px">

Unselect the `Share data about this installation with MediaWiki developers` if you want to disable telemetry. Select the `I'm bored already, just install the wiki` to skip the questionnaire. Press `continue`.

<img src="media/install-5-name-2.png" style="width:600px">

Press `continue` to begin installation (should take only a second to finish).

<img src="media/install-6-continue.png" style="width:600px">

7. Press `continue`

<img src="media/install-7-finish-continue.png" style="width:600px">

8. You are done with the installation. Copy the `LocalSettings.php` file to the root of this project directory. and uncomment the following line in `docker-compose.yml` to use the config:
```
- ./LocalSettings.php:/var/www/html/LocalSettings.php
```

<img src="media/install-8-complete.png" style="width:600px">


# Increase URI size limit of Apache server
You might encounter URI too long error:
```
Failed to parse response JSON: expected value at line 1 column 1
<!DOCTYPE HTML PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html><head>
<title>414 Request-URI Too Long</title>
</head><body>
<h1>Request-URI Too Long</h1>
<p>The requested URL's length exceeds the capacity
limit for this server.<br />
</p>
<hr>
<address>Apache/2.4.57 (Debian) Server at 172.18.0.3 Port 80</address>
</body></html>
```

In the above case, do the following steps to enlarge the URI length limit.

0. Start the MediaWiki container if you haven't already:
```
docker-compose up
```

1. In a new terminal, open a shell in the server:
```
docker-compose exec mediawiki /bin/bash
```

2. Install Nano text editor:
```
apt update
apt install nano
```

3. Open apache config file by doing `nano /etc/apache2/apache2.conf` and insert:

```
LimitRequestLine 4294967296
LimitRequestFieldSize 4294967296
```

Raise the URI byte size limit to 4 GiB.

4. Open another config file by doing `nano /etc/apache2/sites-available/000-default.conf` and insert the same two lines before `</VirtualHost>`:

```
LimitRequestLine 4294967296
LimitRequestFieldSize 4294967296
```

Reference: https://stackoverflow.com/a/57246448/6798201