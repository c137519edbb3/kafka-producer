import datetime
import cv2
import json
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from time import sleep
import base64

from auth import Auth
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

    def send_frame(self, frame_base64, camera_id, organization_id):
        self.producer.send(self.topic_name, {
            'frame': frame_base64,
            'timestamp': str(datetime.datetime.now().timestamp()),
            'camera_id': camera_id,
            'organization_id': organization_id
        })

class CameraProducer(VideoProducer):
    def __init__(self, camera_url, topic_name, camera_id, kafka_bootstrap_servers="34.132.217.189:9092", retry_interval=5, organization_id=1):
        super().__init__(topic_name, kafka_bootstrap_servers)
        self.camera_url = camera_url
        self.retry_interval = retry_interval
        self.camera = None
        self.connected = False
        self.camera_id = camera_id
        self.organization_id = organization_id

    def connect_camera(self):
        try:
            self.camera = cv2.VideoCapture(self.camera_url)
            if self.camera.isOpened():
                self.connected = True
                print(f"Successfully connected to IP camera at {self.camera_url}")
                return True
            return False
        except Exception as e:
            print(f"Camera connection error: {e}")
            return False

    def start_streaming(self):
        retry_count = 0
        try:
            while True:
                if not self.connected or not self.camera.isOpened():
                    retry_count += 1
                    print(f"Camera {self.camera_id}: Attempting to connect (attempt {retry_count})")
                    if self.connect_camera():
                        retry_count = 0
                    else:
                        print(f"Camera {self.camera_id}: Connection failed, retrying in {self.retry_interval} seconds...")
                        sleep(self.retry_interval)
                        continue

                success, frame = self.camera.read()
                if not success:
                    self.connected = False
                    continue
                
                _, buffer = cv2.imencode('.jpg', frame)
                frame_base64 = base64.b64encode(buffer).decode('utf-8')
                self.send_frame(frame_base64, self.camera_id, self.organization_id)
                sleep(1)
                
        except KeyboardInterrupt:
            print(f"Camera {self.camera_id}: Stopping stream...")
        finally:
            if self.camera:
                self.camera.release()
            self.producer.close()

if __name__ == "__main__":
    auth = Auth()
    if not auth.login():
        print("Failed to authenticate with EyeconAI API")
        exit(1)

    cameras_data = auth.get_online_cameras()
    if not cameras_data:
        print("No online cameras found")
        exit(1)


    cameras = [
        {
            "url": camera["ipAddress"],
            "id": camera['cameraId'],
            "location": camera["location"]
        }
        for camera in cameras_data
    ]

    import threading
    threads = []
    
    for camera in cameras:
        producer = CameraProducer(
            camera_url=camera['url'],
            topic_name='camera_feed',
            camera_id=camera['id'],
            organization_id=int(auth.user.get("organization", {}).get("id", 1))
        )
        thread = threading.Thread(target=producer.start_streaming)
        thread.daemon = True
        threads.append(thread)
        thread.start()

    try:
        while True:
            sleep(1)
    except KeyboardInterrupt:
        print("Stopping all streams...")