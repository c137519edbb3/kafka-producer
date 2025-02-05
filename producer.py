import datetime
import random
import cv2
import json
import numpy as np
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from time import sleep
import base64

from config import Config

class VideoProducer:
    def __init__(self, topic_name, bootstrap_servers=['localhost:9092']):
        self.topic_name = topic_name
        self.bootstrap_servers = bootstrap_servers
        self.producer = KafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        self.create_topic_if_not_exists()
    
    def create_topic_if_not_exists(self):
        admin_client = KafkaAdminClient(bootstrap_servers=self.bootstrap_servers)
        try:
            topic = NewTopic(name=self.topic_name, 
                           num_partitions=1, 
                           replication_factor=1)
            admin_client.create_topics([topic])
            print(f"Topic {self.topic_name} created")
        except TopicAlreadyExistsError:
            print(f"Topic {self.topic_name} already exists")
        finally:
            admin_client.close()

    def send_frame(self, frame_base64):
        self.producer.send(self.topic_name, {
            'frame': frame_base64,
            'timestamp': str(datetime.datetime.now())
        })

class CameraProducer(VideoProducer):
    def __init__(self, camera_url, topic_name, kafka_bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVER_URL):
        super().__init__(topic_name, kafka_bootstrap_servers)
        # Connect to IP camera
        self.camera = cv2.VideoCapture(camera_url)
        if not self.camera.isOpened():
            raise Exception(f"Failed to connect to IP camera at {camera_url}")
        print(f"Successfully connected to IP camera at {camera_url}")
        
    def start_streaming(self):
        try:
            while True:
                success, frame = self.camera.read()
                if not success:
                    print("Failed to capture frame from IP camera")
                    sleep(1)
                    continue
                
                # Convert frame to base64 string for sending via Kafka
                _, buffer = cv2.imencode('.jpg', frame)
                frame_base64 = base64.b64encode(buffer).decode('utf-8')
                
                # Send frame to Kafka
                self.send_frame(frame_base64)
                # print('Frame sent to Kafka Broker')
                sleep(1)
                
        except KeyboardInterrupt:
            print("Stopping stream...")
        except Exception as e:
            print(f"Error during streaming: {e}")
        finally:
            print("Closing camera connection...")
            self.camera.release()
            self.producer.close()

if __name__ == "__main__":

    camera_url = "https://192.168.1.102:8080/video"

    producer = CameraProducer(topic_name='camera_feed', camera_url=camera_url)
    producer.start_streaming()