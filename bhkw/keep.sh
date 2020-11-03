
#!/bin/bash
while true
do
    wget -O/dev/null -q http://192.168.192.129/remote.htm?pump=1&valve=1 && mosquitto_pub -t "emon/bhkw1/request_power" -m 2100 && sleep 30
done

