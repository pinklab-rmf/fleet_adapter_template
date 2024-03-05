#!/usr/bin/env python3

import rclpy
from rclpy.node import Node
import threading
from fastapi import FastAPI
import uvicorn
import argparse
import yaml
import sys
from rclpy.qos import qos_profile_sensor_data
from rclpy.qos import QoSProfile
from rclpy.qos import QoSHistoryPolicy
from rclpy.qos import QoSReliabilityPolicy
from rclpy.qos import QoSDurabilityPolicy
from pydantic import BaseModel
from rmf_fleet_msgs.msg import Location, DestinationRequest, ModeRequest, RobotMode, FleetState
import time
import rmf_adapter.vehicletraits as traits
import rmf_adapter.geometry as geometry

# Destination 모델 정의
app = FastAPI()


class Destination(BaseModel):
    x: float
    y: float
    yaw: float
    
class FleetManager(Node):
    def __init__(self, config):
        super().__init__('fleet_manager')
        self.fleet_data = {}
        self.config = config
        self.fleet_name = self.config["rmf_fleet"]["name"]
        self.robot_names = self.config['robots']
        self.task_id = {}
        self.last_destination_command = {}
        self.is_completed = {}
        self.duration = {}
        self.destination = {}
        
        for robot_name in self.robot_names:
            self.is_completed[robot_name] = True
            self.last_destination_command[robot_name] = -1
            self.duration[robot_name] = 0.0
            self.destination[robot_name] = None
            
        profile = traits.Profile(geometry.make_final_convex_circle(
            self.config['rmf_fleet']['profile']['footprint']),
            geometry.make_final_convex_circle(
                self.config['rmf_fleet']['profile']['vicinity']))
        self.vehicle_traits = traits.VehicleTraits(
            linear=traits.Limits(
                *self.config['rmf_fleet']['limits']['linear']),
            angular=traits.Limits(
                *self.config['rmf_fleet']['limits']['angular']),
            profile=profile)
        self.vehicle_traits.differential.reversible =\
            self.config['rmf_fleet']['reversible']
            
        super().__init__(f'{self.fleet_name}_fleet_manager')
        
        @app.post("/open-rmf/rmf_demos_fm/navigate/{robot_name}")
        async def navigate_robot(robot_name: str, destination: Destination):
            # ...
            self.get_logger().debug(f"Received navigation request for {robot_name}")
            self.get_logger().debug(f"Destination: {destination}")
            self.destination[robot_name] = destination
            if self.is_completed[robot_name]:
                msg = DestinationRequest()
                msg.fleet_name = fleet_manager.fleet_name
                msg.robot_name = robot_name
                self.last_destination_command[robot_name] += 1
                msg.destination.x = destination.x
                msg.destination.y = destination.y
                msg.destination.yaw = destination.yaw
                msg.destination.level_name = "L1"
                msg.task_id = str(self.last_destination_command[robot_name])  # Convert task_id to string

                self.is_completed[robot_name] = False

                topic_name = f'/{self.fleet_name}/robot_destination_requests'
                pub = fleet_manager.create_publisher(DestinationRequest, topic_name, 5)
                pub.publish(msg)
                
                self.get_logger().debug(f"Current cmd_id: {self.last_destination_command.get(robot_name)}")
                self.get_logger().debug(f"Published destination request for {robot_name}")
                self.get_logger().debug(f"is_completed: {self.is_completed[robot_name]}")
                return {'success': True, 'msg': ''}
            else:
                return {'success': False, 'msg': f'Robot {robot_name} is not ready for navigation'}
            
        
        @app.post("/open-rmf/rmf_demos_fm/command_completed/{robot_name}")
        async def command_completed(robot_name: str):
            if not self.is_completed[robot_name]:
                response = fleet_manager.complete_task(robot_name, self.destination[robot_name])
                self.get_logger().debug(f"Command completed: {response}")
                if response:
                    self.is_completed[robot_name] = True
                    self.destination[robot_name] = None
                    return {'success': True, 'msg': ''}
                else:
                    return {'success': False, 'msg': f'Robot {robot_name} has not reached the destination'}
            else:
                return {'success': False, 'msg': f'Robot {robot_name} is not ready for navigation'}
            
            
        @app.post("/open-rmf/rmf_demos_fm/stop/{robot_name}")
        async def stop_robot(robot_name: str):
            # 로봇 이름을 키로 사용하여 stop 
            self.get_logger().debug(f"Received stop request for {robot_name}")
            self.is_completed[robot_name] = True
            msg = ModeRequest()
            msg.fleet_name = fleet_manager.fleet_name
            msg.robot_name = robot_name
            msg.mode.mode = RobotMode.MODE_PAUSED
            msg.task_id = str(self.last_destination_command[robot_name])
            topic_name = f'/{self.fleet_name}/robot_mode_requests'
            pub = fleet_manager.create_publisher(ModeRequest, topic_name, 5)
            pub.publish(msg)
            self.get_logger().debug(f"Published stop request for {robot_name}")
            return {'success': True, 'msg': ''}
        
        @app.get('/open-rmf/rmf_demos_fm/status/{robot_name}')
        async def get_status(robot_name: str):
            robot_status = self.fleet_data[robot_name]
            if robot_status:
                data = {
                    'robot_name': robot_name,
                    'map_name': 'L1',
                    'position': robot_status['position'],
                    'battery': float(100), # Minibot 로봇은 배터리 정보를 제공하지 않으므로 임시로 1.0으로 설정
                    'destination_arrival': {
                        'cmd_id': self.last_destination_command.get(robot_name),
                        'duration': self.duration_compute(robot_name, robot_status, self.destination[robot_name], self.vehicle_traits)
                    },
                    'last_completed_request': None,
                    'replan': False
                }
                return {'data': data, 'success': True, 'msg': ''}
            else:
                return {'data': {}, 'success': False, 'msg': f'No data found for {robot_name}'}


    def duration_compute(self, robot_name, robot_status, destination, vehicle_traits):
        if self.destination[robot_name] is None:
            return 0.0
        robot_position = robot_status['position']
        cur_yaw = robot_position['yaw']
        target_yaw = destination.yaw
        distance = self.distance(robot_position['x'], robot_position['y'], destination.x,  destination.y)
        duration = (distance / vehicle_traits.linear.nominal_velocity) +\
               (abs(abs(cur_yaw) - abs(target_yaw)) / vehicle_traits.rotational.nominal_velocity)
        self.get_logger().debug(f"Robot {robot_name} estimated duration: {duration}")
        return duration

        
    def complete_task(self, robot_name, destination):
        robot_position = self.fleet_data[robot_name]['position']
        
        distance = self.distance(robot_position['x'], robot_position['y'], destination.x,  destination.y)
        if distance < 0.5:
            self.is_completed[robot_name] = True
            return True
        else:
            return False        
        
    def distance(self, x1, y1, x2, y2):
        return ((abs(x1 - x2))**2 + (abs(y1 - y2))**2)**0.5

    def add_robot(self, robot_name):
        self.fleet_data[robot_name] = {
            'battery': None,
            'position': None,
            'mode': None,
            'destination_arrival': None,
            'last_completed_request': None,
            'replan': False
        }
        

    def add_fleet(self, fleet_name): 
            qos = QoSProfile(
                history=QoSHistoryPolicy.KEEP_LAST,
                depth=10,
                reliability=QoSReliabilityPolicy.RELIABLE,
                durability=QoSDurabilityPolicy.VOLATILE
            )
            self.create_subscription(
                FleetState,
                f'/{self.fleet_name}/fleet_states',
                self.fleet_state_callback,
                qos
            )

    def fleet_state_callback(self, msg):
        for robot_state in msg.robots:
            robot_name = robot_state.name
            self.get_logger().debug(f"Received fleet state for {robot_name}")
            if robot_name in self.fleet_data:
                self.fleet_data[robot_name] = {
                    'battery': robot_state.battery_percent,
                    'position': {
                        'x': robot_state.location.x,
                        'y': robot_state.location.y,
                        'yaw': robot_state.location.yaw
                    },
                    'mode': robot_state.mode.mode
                }


def main(argv=sys.argv):
    rclpy.init(args=argv)
    global fleet_manager
    args_without_ros = rclpy.utilities.remove_ros_args(argv)

    parser = argparse.ArgumentParser(
        prog="fleet_manager",
        description="Configure and spin up the fleet adapter")
    parser.add_argument("-c", "--config_file", type=str, required=True,
                        help="Path to the config.yaml file")
    parser.add_argument("-n", "--nav_graph", type=str, required=True,
                        help="Path to the nav_graph for this fleet adapter")
    args = parser.parse_args(args_without_ros[1:])

    with open(args.config_file, "r") as f:
        config = yaml.safe_load(f)

    fleet_manager = FleetManager(config)
    fleet_name = config["rmf_fleet"]["name"]
    robot_names = config['robots']

    for robot_name in robot_names:
        fleet_manager.add_robot(robot_name)
    fleet_manager.add_fleet(fleet_name)

    spin_thread = threading.Thread(target=rclpy.spin, args=(fleet_manager,))
    spin_thread.start()

    uvicorn.run(app,
                host=config['rmf_fleet']['fleet_manager']['ip'],
                port=config['rmf_fleet']['fleet_manager']['port'],
                log_level='warning')


if __name__ == '__main__':
    main()
