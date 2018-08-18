from influxdb import InfluxDBClient
import pika
import sys
import json
import time


# declarations--------------------
client = InfluxDBClient(host='localhost', port=8086)
client.create_database('ftest')
client.switch_database('ftest')
#client.create_retention_policy('onehr','1h',1, default=True)
#run one time only
#client.query("CREATE CONTINUOUS QUERY maxtemp_cq ON ftest RESAMPLE EVERY 10s BEGIN SELECT max(temp) as temp, x_axis, y_axis, z_axis, carId INTO ftest.onehr.newcar FROM ftest.oneday.car GROUP BY time(30s) END")

#def send_data_to_server(data):
cred=pika.PlainCredentials('admin', 'password')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='HOST IP',credentials=cred))
print("connection-",connection)
channel = connection.channel()
channel.queue_declare(queue='local_queue', durable=True)



# code----------------------------

def read_from_db():
    print("----------------------\nin read from db")
    client.switch_database('ftest')
    client.query("alter retention policy onehr on ftest duration 1h replication 1 default")
    rs = client.query("SELECT * from newcar")
    #print("\n\nrs----",rs,"\n\n")
    points =list(rs.get_points(measurement='newcar', tags={'carId': '123456'}))
    top_item=(len(points)-1)
    print("\npoints od top---->",points[top_item])
    #print("type",type(points[0])) #should be dict
    carId=points[top_item]['carId']
    temp=points[top_item]['temp']
    Xaxis=points[top_item]['x_axis']
    Yaxis=points[top_item]['y_axis']
    Zaxis=points[top_item]['z_axis']
    json_send={
        "measurement":"car",
        "tags":{
            "carId":carId
            },
        "fields":{
            "temp":float(temp),
            "x_axis":int(Xaxis),
            "y_axis":int(Yaxis),
            "z_axis":int(Zaxis)
            }
        }
    print("return from influx-",json_send)
    return(json_send)


def send_data_to_server(data):
    print("\nsend_data_to_server",data)
    res=channel.basic_publish(exchange='',
                      routing_key='local_queue',
                      #data should be of dict type
                      body=json.dumps(data),
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                                ))
    print(res)

#connection.close()
#send_data_to_server("hey")



while True:
    time.sleep(10)
    data = read_from_db()
    print("\nread from db-\n", data)
    send_data_to_server(data)

