
#!/bin/bash
while true
do
    wget -O/dev/null "http://192.168.192.129/remote.htm?pump=1&comp=0&valve=1" && mosquitto_pub -t "emon/bhkw1/request_power" -m 2000 && sleep 30
done

