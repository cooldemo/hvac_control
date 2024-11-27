screen -L -d -m python3 sController.py
cd bhkw
screen -d -m python3 bhkw.py
cd ..
cd emeter
screen -d -m python3 mbusparse.py
cd ..
cd mppsolar
screen -d -m python3 mppsolar.py
cd ..