from pathlib import Path
from secrets import randbelow
import random
l=Path("C:\\bigdata\\data\\log_data_ip_request.txt")
new_l=Path('C:\\Kloud9-Demo-Project\\project files\\src\\new_log.txt')

p=open(l)
count = 0
increment = 1
increm = 1
new_data=open(new_l,'a')

def incremental():

    p.seek(0)
    for line_num, i in enumerate(p):
        global increment
        global count
        global increm
        l_digit = int(i.strip(" ").split(" ")[0].split(".")[-1])
        time_hour = int(i.strip(" ").split(" ")[3].split(":")[1])
        day_digit = int(i.strip(" ").split(" ")[3].split("/")[0][1:])
        if line_num < 300:
                if (line_num + 1) % 9 == 0:
                    i = i.replace('GET', 'POST', 1)
                    b = randbelow(12)
                    i = i.replace(":0"+str(time_hour), ":0"+str(b),1)

                if (line_num + 1) % 10 == 0:
                    increment += 1
                    increm += 1
                    # incr = int(l_digit + increment)
                    a = randbelow(255)
                    i = i.replace(str(l_digit),str(a),1)
                    b = randbelow(12)
                    # i = i.replace(str(l_digit), str(incr), 1)
                    # incr_hour = int(time_hour + increm)
                    i = i.replace(":0"+str(time_hour), ":0"+str(b),1)

                if (line_num + 1) % 11 == 0:
                    c = random.randint(10,31)
                    i = i.replace(str(day_digit),str(c),1)

        new_data.write(i)
        print(count)
        count = count + 1
        # increment = +1
        # increm = 1

num_record = int(input("Enter the number of records wanted(in Lakhs): "))
num_acc = int((num_record * 1000))

for i in range(num_acc):
    print(str(i+1))
    incremental()
