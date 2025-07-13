import requests
import json
import time
import math
import pandas as pd
import xlsxwriter
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
import asyncio
import aiohttp

@dataclass
class EnhancedTelemetryData:
    """Enhanced comprehensive telemetry data structure"""
    # Basic GPS data
    timestamp: datetime = None
    latitude: float = 0.0
    longitude: float = 0.0
    altitude: float = 0.0
    speed: float = 0.0
    course: float = 0.0
    satellites: int = 0
    hdop: float = 0.0
    
    # Device status
    power_voltage: float = 0.0
    battery_voltage: float = 0.0
    internal_battery: float = 0.0
    gsm_signal: int = 0
    temperature: float = 0.0
    
    # Engine and vehicle data
    engine_on: bool = False
    ignition: bool = False
    odometer: float = 0.0
    engine_hours: float = 0.0
    fuel_level: float = 0.0
    fuel_consumption: float = 0.0
    rpm: int = 0
    coolant_temp: float = 0.0
    oil_pressure: float = 0.0
    
    # Movement and behavior
    acceleration: float = 0.0
    max_acceleration: float = 0.0
    max_braking: float = 0.0
    harsh_acceleration: int = 0
    harsh_braking: int = 0
    harsh_cornering: int = 0
    max_cornering: float = 0.0
    idling_time: float = 0.0
    movement_sensor: int = 0
    
    # Driver and trip data
    driver_id: str = "0"
    driver_name: str = ""
    trip_id: str = ""
    
    # Digital inputs/outputs
    digital_inputs: Dict[str, bool] = field(default_factory=dict)
    digital_outputs: Dict[str, bool] = field(default_factory=dict)
    
    # Analog inputs
    analog_inputs: Dict[str, float] = field(default_factory=dict)
    
    # Custom sensors
    custom_sensors: Dict[str, Any] = field(default_factory=dict)
    
    # CAN bus data
    can_data: Dict[str, Any] = field(default_factory=dict)
    
    # Enhanced fields
    speeding_violations: int = 0
    eco_driving_score: float = 0.0
    maintenance_alerts: List[str] = field(default_factory=list)
    geofence_events: List[str] = field(default_factory=list)
    
    # Raw message data
    raw_parameters: Dict[str, Any] = field(default_factory=dict)

class EnhancedWialonExtractor:
    def __init__(self, token, base_url="https://hst-api.wialon.com"):
        self.base_url = base_url
        self.session_id = None
        self.token = token
        self.unit_sensors = {}
        self.unit_info = {}
        self.drivers_info = {}
        self.geofences = {}

    async def login(self):
        """Async login using token"""
        url = f"{self.base_url}/wialon/ajax.html"
        params = {
            'svc': 'token/login',
            'params': json.dumps({'token': self.token})
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=params) as response:
                result = await response.json()
                if 'error' in result:
                    raise Exception(f"Login failed: {result}")
                self.session_id = result['eid']
                print(f"✅ Logged in. Session ID: {self.session_id}")
                return result

    def login_sync(self):
        """Synchronous login for compatibility"""
        url = f"{self.base_url}/wialon/ajax.html"
        params = {
            'svc': 'token/login',
            'params': json.dumps({'token': self.token})
        }
        response = requests.post(url, data=params)
        result = response.json()
        if 'error' in result:
            raise Exception(f"Login failed: {result}")
        self.session_id = result['eid']
        print(f"✅ Logged in. Session ID: {self.session_id}")
        return result

    def logout(self):
        """Logout"""
        if self.session_id:
            url = f"{self.base_url}/wialon/ajax.html"
            params = {
                'svc': 'core/logout',
                'params': '{}',
                'sid': self.session_id
            }
            requests.post(url, data=params)
            print("✅ Logged out")
            self.session_id = None

    def make_request(self, service, params={}):
        """Make API request with enhanced error handling"""
        if not self.session_id:
            raise Exception("Not logged in")
            
        url = f"{self.base_url}/wialon/ajax.html"
        data = {
            'svc': service,
            'params': json.dumps(params),
            'sid': self.session_id
        }
        
        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = requests.post(url, data=data, timeout=30)
                result = response.json()
                
                if 'error' in result:
                    if result['error'] == 1:  # Invalid session
                        print("Session expired, re-logging in...")
                        self.login_sync()
                        data['sid'] = self.session_id
                        continue
                    else:
                        raise Exception(f"API Error in {service}: {result}")
                        
                return result
                
            except (requests.RequestException, json.JSONDecodeError) as e:
                if attempt == max_retries - 1:
                    raise Exception(f"Request failed after {max_retries} attempts: {e}")
                time.sleep(2 ** attempt)  # Exponential backoff
                
        return None

    def get_all_units(self):
        """Get all units with comprehensive flags"""
        print("📋 Getting all units...")
        try:
            units_params = {
                "spec": {
                    "itemsType": "avl_unit",
                    "propName": "sys_name",
                    "propValueMask": "*",
                    "sortType": "sys_name"
                },
                "force": 1,
                "flags": 0x00000001 | 0x00000002 | 0x00000008 | 0x00000020 | 0x00000040 | 0x00000200 | 0x00000400 | 0x00001000 | 0x00008000,
                "from": 0,
                "to": 0
            }
            
            result = self.make_request('core/search_items', units_params)
            units = result.get('items', [])
            
            print(f"   ✅ Found {len(units)} units")
            
            # Store unit information
            for unit in units:
                self.unit_info[unit['id']] = {
                    'name': unit.get('nm', ''),
                    'device_type': unit.get('hw', ''),
                    'unique_id': unit.get('uid', ''),
                    'phone': unit.get('ph', ''),
                    'sensors': unit.get('sens', {}),
                    'custom_fields': unit.get('flds', {}),
                    'admin_fields': unit.get('aflds', {}),
                    'profile_fields': unit.get('pflds', {}),
                    'counters': unit.get('cntrs', {}),
                    'maintenance': unit.get('mnt', {}),
                    'driver_units': unit.get('drvrs', {}),
                    'trailers': unit.get('trl', {}),
                    'equipment': unit.get('eqp', {})
                }
                self.unit_sensors[unit['id']] = unit.get('sens', {})
            
            return units
            
        except Exception as e:
            print(f"   ❌ Error getting units: {e}")
            return []

    def get_drivers(self):
        """Get all drivers information"""
        print("👥 Getting drivers information...")
        try:
            drivers_params = {
                "spec": {
                    "itemsType": "avl_driver",
                    "propName": "sys_name",
                    "propValueMask": "*",
                    "sortType": "sys_name"
                },
                "force": 1,
                "flags": 0x00000001,
                "from": 0,
                "to": 0
            }
            
            result = self.make_request('core/search_items', drivers_params)
            drivers = result.get('items', [])
            
            for driver in drivers:
                self.drivers_info[driver['id']] = {
                    'name': driver.get('nm', ''),
                    'code': driver.get('c', ''),
                    'phone': driver.get('ph', ''),
                    'email': driver.get('email', ''),
                    'custom_fields': driver.get('flds', {})
                }
            
            print(f"   ✅ Found {len(drivers)} drivers")
            return drivers
            
        except Exception as e:
            print(f"   ❌ Error getting drivers: {e}")
            return []

    def get_enhanced_messages(self, unit_id, time_from, time_to):
        """Get enhanced messages with all available data"""
        print(f"📡 Extracting enhanced messages for unit {unit_id}...")
        try:
            messages_params = {
                "itemId": unit_id,
                "timeFrom": time_from,
                "timeTo": time_to,
                "flags": 0,
                "flagsMask": 65535,  # All flags
                "loadCount": 10000   # Increased load count
            }
            
            result = self.make_request('messages/load_interval', messages_params)
            messages = result.get('messages', [])
            
            print(f"   ✅ Found {len(messages)} messages")
            return messages
            
        except Exception as e:
            print(f"   ❌ Error getting messages: {e}")
            return []

    def get_trips_data(self, unit_id, time_from, time_to):
        """Get trips data using reports"""
        print(f"🚗 Getting trips data for unit {unit_id}...")
        try:
            # Use report template for trips
            report_params = {
                "reportResourceId": unit_id,
                "reportTemplateId": 1,
                "reportTemplate": {
                    "n": "trips_report",
                    "ct": "avl_unit",
                    "p": {
                        "grouping": json.dumps({"type": "day"}),
                        "trips": json.dumps({"type": "all"}),
                        "duration": 300,  # Minimum trip duration in seconds
                        "filter": json.dumps({"type": "all"})
                    }
                },
                "interval": {
                    "from": time_from,
                    "to": time_to,
                    "flags": 0
                }
            }
            
            result = self.make_request('report/exec_report', report_params)
            
            if result and 'reportResult' in result:
                trips = result['reportResult'].get('tables', [])
                print(f"   ✅ Found {len(trips)} trip records")
                return trips
            else:
                print("   ⚠️ No trips data available")
                return []
                
        except Exception as e:
            print(f"   ❌ Error getting trips: {e}")
            return []

    def get_events_data(self, unit_id, time_from, time_to):
        """Get various event types"""
        print(f"⚡ Getting events data for unit {unit_id}...")
        events_data = {}
        
        event_types = [
            'maintenance',
            'speeding',
            'geofence',
            'driver_change',
            'fuel_theft',
            'panic_button'
        ]
        
        for event_type in event_types:
            try:
                events_params = {
                    "itemId": unit_id,
                    "timeFrom": time_from,
                    "timeTo": time_to,
                    "flags": 0,
                    "flagsMask": 65535,
                    "loadCount": 1000
                }
                
                result = self.make_request('avl_evts', events_params)
                events_data[event_type] = result.get('events', [])
                
            except Exception as e:
                print(f"   ⚠️ Error getting {event_type} events: {e}")
                events_data[event_type] = []
        
        return events_data

    def parse_enhanced_message(self, msg, unit_id) -> EnhancedTelemetryData:
        """Parse a single message with enhanced data extraction"""
        telemetry = EnhancedTelemetryData()
        
        if not msg or not isinstance(msg, dict):
            return telemetry
            
        # Basic message data
        telemetry.timestamp = datetime.fromtimestamp(msg.get('t', 0))
        
        # Position data
        pos = msg.get('pos', {})
        if pos:
            telemetry.latitude = pos.get('y', 0.0)
            telemetry.longitude = pos.get('x', 0.0)
            telemetry.altitude = pos.get('z', 0.0)
            telemetry.speed = pos.get('s', 0.0)
            telemetry.course = pos.get('c', 0.0)
            telemetry.satellites = pos.get('sc', 0)
            telemetry.hdop = pos.get('hdop', 0.0)
        
        # Parameters
        params = msg.get('p', {})
        telemetry.raw_parameters = params.copy()
        
        # Enhanced parameter mapping with more comprehensive coverage
        parameter_mapping = {
            # Power and electrical
            'pwr_ext': 'power_voltage',
            'pwr_int': 'battery_voltage',
            'battery': 'battery_voltage',
            'int_battery': 'internal_battery',
            'gsm_signal': 'gsm_signal',
            'gsm_level': 'gsm_signal',
            'pcb_temp': 'temperature',
            'temperature': 'temperature',
            'temp1': 'temperature',
            
            # Engine and vehicle - expanded
            'engine_on': 'engine_on',
            'ignition': 'ignition',
            'ign': 'ignition',
            'acc': 'ignition',
            'mileage': 'odometer',
            'odometer': 'odometer',
            'engine_hours': 'engine_hours',
            'eh': 'engine_hours',
            'fuel_level': 'fuel_level',
            'fuel_lvl': 'fuel_level',
            'fuel1': 'fuel_level',
            'fuel_consumption': 'fuel_consumption',
            'fuel_cons': 'fuel_consumption',
            'rpm': 'rpm',
            'engine_rpm': 'rpm',
            'coolant_temp': 'coolant_temp',
            'engine_temp': 'coolant_temp',
            'oil_pressure': 'oil_pressure',
            'oil_press': 'oil_pressure',
            
            # Movement and behavior - expanded
            'acceleration': 'acceleration',
            'acc_x': 'acceleration',
            'max_acceleration': 'max_acceleration',
            'max_acc': 'max_acceleration',
            'max_braking': 'max_braking',
            'max_brake': 'max_braking',
            'harsh_acceleration': 'harsh_acceleration',
            'harsh_acc': 'harsh_acceleration',
            'harsh_braking': 'harsh_braking',
            'harsh_brake': 'harsh_braking',
            'harsh_cornering': 'harsh_cornering',
            'harsh_turn': 'harsh_cornering',
            'wln_crn_max': 'max_cornering',
            'cornering': 'max_cornering',
            'idling_time': 'idling_time',
            'idle_time': 'idling_time',
            'movement_sens': 'movement_sensor',
            'movement': 'movement_sensor',
            
            # Driver and trip
            'avl_driver': 'driver_id',
            'driver_code': 'driver_id',
            'driver_id': 'driver_id',
            'trip_id': 'trip_id',
            'trip': 'trip_id',
            
            # Additional vehicle parameters
            'door_1': 'digital_inputs',
            'door_2': 'digital_inputs',
            'panic': 'digital_inputs',
            'sos': 'digital_inputs',
            'tilt': 'analog_inputs',
            'vibration': 'analog_inputs',
            'ext_temp': 'analog_inputs',
            'humidity': 'analog_inputs',
        }
        
        # Apply enhanced parameter mapping
        for param_key, param_value in params.items():
            if param_key in parameter_mapping:
                attr_name = parameter_mapping[param_key]
                if attr_name in ['engine_on', 'ignition']:
                    setattr(telemetry, attr_name, bool(param_value))
                elif attr_name not in ['digital_inputs', 'analog_inputs']:
                    setattr(telemetry, attr_name, param_value)
        
        # Enhanced digital inputs parsing
        for key, value in params.items():
            if key.startswith('din') and key[3:].isdigit():
                telemetry.digital_inputs[key] = bool(value)
            elif key.startswith('door'):
                telemetry.digital_inputs[key] = bool(value)
            elif key in ['panic', 'sos', 'alarm']:
                telemetry.digital_inputs[key] = bool(value)
        
        # Enhanced digital outputs parsing
        for key, value in params.items():
            if key.startswith('dout') and key[4:].isdigit():
                telemetry.digital_outputs[key] = bool(value)
            elif key.startswith('relay'):
                telemetry.digital_outputs[key] = bool(value)
        
        # Enhanced analog inputs parsing
        for key, value in params.items():
            if key.startswith('ain') and key[3:].isdigit():
                telemetry.analog_inputs[key] = float(value)
            elif key in ['tilt', 'vibration', 'ext_temp', 'humidity', 'pressure']:
                telemetry.analog_inputs[key] = float(value)
        
        # Enhanced CAN bus data parsing
        can_keys = [k for k in params.keys() if k.startswith('can_') or k.startswith('j1939_')]
        for key in can_keys:
            telemetry.can_data[key] = params[key]
        
        # Custom sensors - enhanced mapping
        if unit_id in self.unit_sensors:
            for sensor_id, sensor_info in self.unit_sensors[unit_id].items():
                sensor_name = sensor_info.get('n', f'sensor_{sensor_id}')
                param_name = sensor_info.get('p', '')
                if param_name and param_name in params:
                    telemetry.custom_sensors[sensor_name] = params[param_name]
        
        # Calculate derived metrics
        telemetry.speeding_violations = 1 if telemetry.speed > 80 else 0  # Configurable threshold
        
        # Calculate eco-driving score (simplified)
        eco_score = 100
        if telemetry.harsh_acceleration > 0:
            eco_score -= (telemetry.harsh_acceleration * 5)
        if telemetry.harsh_braking > 0:
            eco_score -= (telemetry.harsh_braking * 5)
        if telemetry.harsh_cornering > 0:
            eco_score -= (telemetry.harsh_cornering * 3)
        if telemetry.speed > 100:
            eco_score -= 10
        telemetry.eco_driving_score = max(0, eco_score)
        
        # Check for maintenance alerts
        if telemetry.engine_hours > 0 and telemetry.engine_hours % 100 < 1:  # Every 100 hours
            telemetry.maintenance_alerts.append("Engine maintenance due")
        if telemetry.odometer > 0 and telemetry.odometer % 10000 < 100:  # Every 10000 km
            telemetry.maintenance_alerts.append("Vehicle service due")
        if telemetry.fuel_level < 10:
            telemetry.maintenance_alerts.append("Low fuel level")
        if telemetry.power_voltage < 11000:  # Below 11V
            telemetry.maintenance_alerts.append("Low battery voltage")
        
        return telemetry

    def generate_ptt_excel_report(self, units_data, date_range, report_type="weekly"):
        """Generate Excel report matching PTT template exactly"""
        print("📊 Generating PTT Excel Report...")
        
        filename = f"PTT_Fleet_Report_{date_range['from']}_{date_range['to']}_{report_type}.xlsx"
        
        # Create workbook and worksheets
        workbook = xlsxwriter.Workbook(filename)
        
        # Define formats
        title_format = workbook.add_format({
            'bold': True,
            'font_size': 16,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#D9E1F2'
        })
        
        header_format = workbook.add_format({
            'bold': True,
            'font_size': 12,
            'align': 'center',
            'valign': 'vcenter',
            'bg_color': '#B4C6E7',
            'border': 1
        })
        
        data_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1
        })
        
        number_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'num_format': '#,##0.00'
        })
        
        time_format = workbook.add_format({
            'align': 'center',
            'valign': 'vcenter',
            'border': 1,
            'num_format': 'hh:mm:ss'
        })
        
        # Create Driver Performance sheet
        self._create_driver_performance_sheet(workbook, units_data, date_range, 
                                            title_format, header_format, data_format, 
                                            number_format, time_format)
        
        # Create Vehicle Performance sheet
        self._create_vehicle_performance_sheet(workbook, units_data, date_range,
                                             title_format, header_format, data_format,
                                             number_format, time_format)
        
        # Create Traffic Light Performance sheet
        self._create_traffic_light_performance_sheet(workbook, units_data,
                                                   title_format, header_format, data_format)
        
        workbook.close()
        print(f"✅ Excel report generated: {filename}")
        return filename

    def _create_driver_performance_sheet(self, workbook, units_data, date_range,
                                       title_format, header_format, data_format,
                                       number_format, time_format):
        """Create driver performance sheet matching PTT template"""
        
        sheet = workbook.add_worksheet("Driver Performance")
        
        # Set column widths
        sheet.set_column('A:A', 20)  # Driver Assignment
        sheet.set_column('B:B', 30)  # Driver Name
        sheet.set_column('C:AF', 12)  # Data columns
        
        # Title
        sheet.merge_range('B1:AF1', "Driver's Performance Summary", title_format)
        
        # Week/Period info
        sheet.merge_range('I4:L4', f"Week {report_type.capitalize()}", header_format)
        
        # Date range
        sheet.write('I6', 'DATE FROM:', header_format)
        sheet.write('I7', date_range['from'], data_format)
        sheet.write('U6', 'DATE TO:', header_format)
        sheet.write('U7', date_range['to'], data_format)
        
        # Headers row 10-11
        headers_row1 = [
            "DRIVER'S ASSIGNMENT", "DRIVER'S NAME", "Raw", "Raw", "Raw", "Raw",
            "TOTAL DISTANCE(KM)", "", "TOTAL DRIVING HOURS", "", "Idling", "",
            "ENGINE HOURS", "", "", "SPEEDING DURATION", "OVERSPEEDING VIOLATION",
            "", "", "", "", "", "", "", "", "HARSH\nACCELERATION", "HARSH\nBRAKING",
            "HARSH\nTURNING", "TOTAL", "Date", "Action Taken", "Signature"
        ]
        
        headers_row2 = [
            "", "", "Mileage", "Driving Hours", "Idling Duration", "Engine Hours",
            "", "", "", "", "Duration", "", "", "", "", "", "15", "35", "45", "55",
            "60", "65", "75", "80", "Total", "", "", "", "", "", "", ""
        ]
        
        # Write headers
        for col, header in enumerate(headers_row1):
            sheet.write(9, col, header, header_format)
        
        for col, header in enumerate(headers_row2):
            sheet.write(10, col, header, header_format)
        
        # Write data for each driver/unit
        row = 12  # Start data from row 13 (0-indexed)
        
        for unit_data in units_data:
            if unit_data.get('error'):
                continue
                
            metrics = unit_data.get('metrics', {})
            telemetry_data = unit_data.get('telemetry_data', [])
            
            # Calculate driver-specific metrics
            driver_metrics = self._calculate_driver_metrics(telemetry_data)
            
            # Write row data
            row_data = [
                "PTT TANKER DRIVERS",  # Assignment
                f"Driver for {unit_data['name']}",  # Driver name
                metrics.get('totalDistance', 0),  # Raw mileage
                metrics.get('drivingHours', 0),  # Raw driving hours
                metrics.get('totalIdlingTime', 0),  # Raw idling
                metrics.get('totalEngineHours', 0),  # Raw engine hours
                metrics.get('totalDistance', 0),  # Total distance
                metrics.get('drivingHours', 0) / 24 if metrics.get('drivingHours', 0) > 0 else 0,  # Days format
                metrics.get('drivingHours', 0) / 24 if metrics.get('drivingHours', 0) > 0 else 0,  # Driving hours
                0,  # Extra column
                metrics.get('totalIdlingTime', 0) / 24 if metrics.get('totalIdlingTime', 0) > 0 else 0,  # Idling duration
                0,  # Extra column
                metrics.get('totalEngineHours', 0) / 24 if metrics.get('totalEngineHours', 0) > 0 else 0,  # Engine hours
                0, 0,  # Extra columns
                driver_metrics.get('speeding_duration', 0),  # Speeding duration
                # Speed violation brackets
                driver_metrics.get('speed_violations', {}).get('15-35', 0),
                driver_metrics.get('speed_violations', {}).get('35-45', 0),
                driver_metrics.get('speed_violations', {}).get('45-55', 0),
                driver_metrics.get('speed_violations', {}).get('55-60', 0),
                driver_metrics.get('speed_violations', {}).get('60-65', 0),
                driver_metrics.get('speed_violations', {}).get('65-75', 0),
                driver_metrics.get('speed_violations', {}).get('75-80', 0),
                driver_metrics.get('speed_violations', {}).get('80+', 0),
                sum(driver_metrics.get('speed_violations', {}).values()),  # Total violations
                driver_metrics.get('harsh_acceleration', 0),
                driver_metrics.get('harsh_braking', 0),
                driver_metrics.get('harsh_turning', 0),
                driver_metrics.get('total_harsh_events', 0),
                date_range['to'],  # Date
                "",  # Action taken
                ""   # Signature
            ]
            
            for col, value in enumerate(row_data):
                if isinstance(value, (int, float)) and col > 1:
                    sheet.write(row, col, value, number_format)
                else:
                    sheet.write(row, col, value, data_format)
            
            row += 1

    def _create_vehicle_performance_sheet(self, workbook, units_data, date_range,
                                        title_format, header_format, data_format,
                                        number_format, time_format):
        """Create vehicle performance sheet matching PTT template"""
        
        sheet = workbook.add_worksheet("Vehicle Performance")
        
        # Set column widths
        sheet.set_column('A:A', 15)  # Department
        sheet.set_column('B:B', 10)  # Type
        sheet.set_column('C:C', 15)  # Vehicle No
        sheet.set_column('D:AH', 12)  # Data columns
        
        # Title
        sheet.merge_range('C1:AH1', "Vehicle Performance Summary", title_format)
        
        # Date range
        sheet.write('J5', 'DATE FROM:', header_format)
        sheet.write('J6', date_range['from'], data_format)
        sheet.write('V5', 'DATE TO:', header_format)
        sheet.write('V6', date_range['to'], data_format)
        
        # Headers
        headers_row1 = [
            "Department", "", "Vehicle No.", "Raw", "Raw", "Raw", "Raw",
            "TOTAL DISTANCE(KM)", "", "TOTAL DRIVING HOURS", "", "Idling", "",
            "ENGINE HOURS", "", "", "SPEEDING DURATION", "OVERSPEEDING VIOLATION",
            "", "", "", "", "", "", "", "", "HARSH\nACCELERATION", "HARSH\nBRAKING",
            "HARSH\nTURNING", "TOTAL", "FUEL CONSUMPTION (LITRE)", "", "",
            "TOTAL CO2 EMISSION (KG)"
        ]
        
        headers_row2 = [
            "", "", "", "Mileage", "Driving Hours", "Idling Duration", "Engine Hours",
            "", "", "", "", "Duration", "", "", "", "", "", "15", "35", "45", "55",
            "60", "65", "75", "80", "Total", "", "", "", "", "DRIVING HOURS",
            "IDLING DURATION", "ENGINE HOURS", ""
        ]
        
        # Write headers
        for col, header in enumerate(headers_row1):
            sheet.write(8, col, header, header_format)
        
        for col, header in enumerate(headers_row2):
            sheet.write(9, col, header, header_format)
        
        # Write data for each vehicle
        row = 10  # Start data from row 11 (0-indexed)
        
        for unit_data in units_data:
            if unit_data.get('error'):
                continue
                
            metrics = unit_data.get('metrics', {})
            telemetry_data = unit_data.get('telemetry_data', [])
            
            # Calculate vehicle-specific metrics
            vehicle_metrics = self._calculate_vehicle_metrics(telemetry_data)
            
            # Write row data
            row_data = [
                "PTT TANKER",  # Department
                "TANKER",  # Type
                unit_data['name'],  # Vehicle number
                metrics.get('totalDistance', 0),  # Raw mileage
                metrics.get('drivingHours', 0),  # Raw driving hours
                metrics.get('totalIdlingTime', 0),  # Raw idling
                metrics.get('totalEngineHours', 0),  # Raw engine hours
                metrics.get('totalDistance', 0),  # Total distance
                metrics.get('drivingHours', 0) / 24 if metrics.get('drivingHours', 0) > 0 else 0,  # Days
                metrics.get('drivingHours', 0) / 24 if metrics.get('drivingHours', 0) > 0 else 0,  # Driving hours
                0,  # Extra column
                metrics.get('totalIdlingTime', 0) / 24 if metrics.get('totalIdlingTime', 0) > 0 else 0,  # Idling
                0,  # Extra column
                metrics.get('totalEngineHours', 0) / 24 if metrics.get('totalEngineHours', 0) > 0 else 0,  # Engine hours
                0, 0,  # Extra columns
                vehicle_metrics.get('speeding_duration', 0),  # Speeding duration
                # Speed violation brackets
                vehicle_metrics.get('speed_violations', {}).get('15-35', 0),
                vehicle_metrics.get('speed_violations', {}).get('35-45', 0),
                vehicle_metrics.get('speed_violations', {}).get('45-55', 0),
                vehicle_metrics.get('speed_violations', {}).get('55-60', 0),
                vehicle_metrics.get('speed_violations', {}).get('60-65', 0),
                vehicle_metrics.get('speed_violations', {}).get('65-75', 0),
                vehicle_metrics.get('speed_violations', {}).get('75-80', 0),
                vehicle_metrics.get('speed_violations', {}).get('80+', 0),
                sum(vehicle_metrics.get('speed_violations', {}).values()),  # Total violations
                vehicle_metrics.get('harsh_acceleration', 0),
                vehicle_metrics.get('harsh_braking', 0),
                vehicle_metrics.get('harsh_turning', 0),
                vehicle_metrics.get('total_harsh_events', 0),
                metrics.get('fuelConsumption', 0),  # Fuel consumption
                metrics.get('drivingHours', 0),  # Fuel by driving hours
                metrics.get('totalIdlingTime', 0),  # Fuel by idling
                metrics.get('co2Emission', 0)  # CO2 emission
            ]
            
            for col, value in enumerate(row_data):
                if isinstance(value, (int, float)) and col > 2:
                    sheet.write(row, col, value, number_format)
                else:
                    sheet.write(row, col, value, data_format)
            
            row += 1

    def _create_traffic_light_performance_sheet(self, workbook, units_data,
                                              title_format, header_format, data_format):
        """Create traffic light performance index sheet"""
        
        sheet = workbook.add_worksheet("Traffic Light Performance")
        
        # Set column widths
        sheet.set_column('A:F', 20)
        
        # Title
        sheet.merge_range('A1:F1', "Traffic Light Performance Index", title_format)
        
        # Headers
        headers = [
            "Vehicle/Driver", "Overall Score", "Eco Driving", "Safety Score", 
            "Efficiency Score", "Performance Level"
        ]
        
        for col, header in enumerate(headers):
            sheet.write(2, col, header, header_format)
        
        # Calculate performance scores for each unit
        row = 3
        for unit_data in units_data:
            if unit_data.get('error'):
                continue
                
            metrics = unit_data.get('metrics', {})
            telemetry_data = unit_data.get('telemetry_data', [])
            
            # Calculate performance scores
            performance = self._calculate_performance_scores(telemetry_data, metrics)
            
            # Determine traffic light color
            overall_score = performance['overall_score']
            if overall_score >= 80:
                performance_level = "🟢 EXCELLENT"
                bg_color = '#92D050'
            elif overall_score >= 60:
                performance_level = "🟡 GOOD"
                bg_color = '#FFFF00'
            else:
                performance_level = "🔴 NEEDS IMPROVEMENT"
                bg_color = '#FF0000'
            
            # Create format for this row
            row_format = workbook.add_format({
                'align': 'center',
                'valign': 'vcenter',
                'border': 1,
                'bg_color': bg_color
            })
            
            row_data = [
                unit_data['name'],
                f"{overall_score:.1f}%",
                f"{performance['eco_score']:.1f}%",
                f"{performance['safety_score']:.1f}%",
                f"{performance['efficiency_score']:.1f}%",
                performance_level
            ]
            
            for col, value in enumerate(row_data):
                sheet.write(row, col, value, row_format)
            
            row += 1

    def _calculate_driver_metrics(self, telemetry_data):
        """Calculate driver-specific metrics"""
        if not telemetry_data:
            return {}
        
        # Speed violations by brackets
        speed_violations = {
            '15-35': 0, '35-45': 0, '45-55': 0, '55-60': 0,
            '60-65': 0, '65-75': 0, '75-80': 0, '80+': 0
        }
        
        harsh_acceleration = sum(d.harsh_acceleration for d in telemetry_data)
        harsh_braking = sum(d.harsh_braking for d in telemetry_data)
        harsh_turning = sum(d.harsh_cornering for d in telemetry_data)
        
        speeding_duration = 0
        
        for data in telemetry_data:
            speed = data.speed
            if 15 <= speed < 35:
                speed_violations['15-35'] += 1
            elif 35 <= speed < 45:
                speed_violations['35-45'] += 1
            elif 45 <= speed < 55:
                speed_violations['45-55'] += 1
            elif 55 <= speed < 60:
                speed_violations['55-60'] += 1
            elif 60 <= speed < 65:
                speed_violations['60-65'] += 1
            elif 65 <= speed < 75:
                speed_violations['65-75'] += 1
            elif 75 <= speed < 80:
                speed_violations['75-80'] += 1
            elif speed >= 80:
                speed_violations['80+'] += 1
                speeding_duration += 1  # Count as speeding time
        
        return {
            'speed_violations': speed_violations,
            'speeding_duration': speeding_duration * 5 / 3600,  # Convert to hours
            'harsh_acceleration': harsh_acceleration,
            'harsh_braking': harsh_braking,
            'harsh_turning': harsh_turning,
            'total_harsh_events': harsh_acceleration + harsh_braking + harsh_turning
        }

    def _calculate_vehicle_metrics(self, telemetry_data):
        """Calculate vehicle-specific metrics (same as driver for this template)"""
        return self._calculate_driver_metrics(telemetry_data)

    def _calculate_performance_scores(self, telemetry_data, metrics):
        """Calculate performance scores for traffic light system"""
        if not telemetry_data:
            return {
                'overall_score': 0, 'eco_score': 0, 
                'safety_score': 0, 'efficiency_score': 0
            }
        
        # Eco driving score (based on harsh events and speeding)
        total_harsh = sum(d.harsh_acceleration + d.harsh_braking + d.harsh_cornering 
                         for d in telemetry_data)
        eco_score = max(0, 100 - (total_harsh * 2))
        
        # Safety score (based on speeding violations and harsh events)
        speeding_count = sum(1 for d in telemetry_data if d.speed > 80)
        safety_score = max(0, 100 - (speeding_count * 0.5) - (total_harsh * 1.5))
        
        # Efficiency score (based on idling time and fuel consumption)
        total_idling = sum(d.idling_time for d in telemetry_data)
        idling_hours = total_idling / 3600
        efficiency_score = max(0, 100 - (idling_hours * 5))
        
        # Overall score (weighted average)
        overall_score = (eco_score * 0.3 + safety_score * 0.4 + efficiency_score * 0.3)
        
        return {
            'overall_score': overall_score,
            'eco_score': eco_score,
            'safety_score': safety_score,
            'efficiency_score': efficiency_score
        }

    def extract_comprehensive_fleet_data(self, date_range, report_type="weekly"):
        """Extract comprehensive fleet data for all units"""
        print(f"\n🚀 COMPREHENSIVE FLEET DATA EXTRACTION")
        print(f"📅 Date Range: {date_range['from']} to {date_range['to']}")
        print(f"📊 Report Type: {report_type}")
        print("=" * 80)
        
        # Calculate time range
        time_from = int(datetime.strptime(date_range['from'], "%Y-%m-%d").timestamp())
        time_to = int(datetime.strptime(date_range['to'], "%Y-%m-%d").timestamp())
        
        # Get all units
        units = self.get_all_units()
        if not units:
            print("❌ No units found")
            return None
        
        # Get drivers information
        self.get_drivers()
        
        # Extract data for each unit
        units_data = []
        total_units = len(units)
        
        for idx, unit in enumerate(units):
            unit_id = unit['id']
            unit_name = unit['nm']
            
            print(f"\n📡 Processing Unit {idx + 1}/{total_units}: {unit_name}")
            print("-" * 60)
            
            try:
                # Get enhanced messages
                messages = self.get_enhanced_messages(unit_id, time_from, time_to)
                
                # Parse all messages
                telemetry_data = []
                for msg in messages:
                    parsed_msg = self.parse_enhanced_message(msg, unit_id)
                    telemetry_data.append(parsed_msg)
                
                print(f"   ✅ Parsed {len(telemetry_data)} telemetry records")
                
                # Get trips data
                trips_data = self.get_trips_data(unit_id, time_from, time_to)
                
                # Get events data
                events_data = self.get_events_data(unit_id, time_from, time_to)
                
                # Calculate comprehensive metrics
                metrics = self.calculate_comprehensive_metrics(telemetry_data)
                
                # Store unit data
                unit_data = {
                    'id': unit_id,
                    'name': unit_name,
                    'device_info': self.unit_info.get(unit_id, {}),
                    'telemetry_data': telemetry_data,
                    'trips_data': trips_data,
                    'events_data': events_data,
                    'metrics': metrics,
                    'last_message': telemetry_data[-1] if telemetry_data else {},
                    'data_quality': self.assess_data_quality(telemetry_data)
                }
                
                units_data.append(unit_data)
                
                # Print unit summary
                print(f"   📊 Distance: {metrics.get('totalDistance', 0):.2f} km")
                print(f"   ⏱️  Driving Hours: {metrics.get('drivingHours', 0):.2f} h")
                print(f"   ⚡ Max Speed: {metrics.get('maxSpeed', 0):.1f} km/h")
                print(f"   🚨 Harsh Events: {metrics.get('totalHarshEvents', 0)}")
                print(f"   ⛽ Fuel Consumed: {metrics.get('fuelConsumption', 0):.2f} L")
                
            except Exception as e:
                print(f"   ❌ Error processing unit {unit_name}: {e}")
                units_data.append({
                    'id': unit_id,
                    'name': unit_name,
                    'error': str(e),
                    'telemetry_data': [],
                    'metrics': {},
                    'data_quality': {}
                })
        
        # Generate comprehensive report
        fleet_data = {
            'extraction_info': {
                'date_range': date_range,
                'report_type': report_type,
                'extraction_timestamp': datetime.now().isoformat(),
                'total_units': len(units),
                'successful_units': len([u for u in units_data if not u.get('error')]),
                'failed_units': len([u for u in units_data if u.get('error')])
            },
            'units_data': units_data,
            'fleet_summary': self.calculate_fleet_summary(units_data),
            'data_quality_report': self.generate_fleet_data_quality_report(units_data)
        }
        
        # Generate Excel report
        excel_filename = self.generate_ptt_excel_report(units_data, date_range, report_type)
        fleet_data['excel_report'] = excel_filename
        
        # Print final summary
        self.print_fleet_summary(fleet_data)
        
        return fleet_data

    def calculate_comprehensive_metrics(self, telemetry_data):
        """Calculate comprehensive metrics with enhanced calculations"""
        if not telemetry_data:
            return {}
        
        # Basic distance and time calculations
        total_distance = 0
        if len(telemetry_data) > 1:
            first_odometer = telemetry_data[0].odometer
            last_odometer = telemetry_data[-1].odometer
            total_distance = (last_odometer - first_odometer) / 1000  # Convert to km
        
        # Speed analysis
        speeds = [d.speed for d in telemetry_data if d.speed > 0]
        max_speed = max(speeds) if speeds else 0
        avg_speed = sum(speeds) / len(speeds) if speeds else 0
        
        # Engine analysis
        engine_on_count = sum(1 for d in telemetry_data if d.engine_on)
        total_messages = len(telemetry_data)
        engine_on_percentage = (engine_on_count / total_messages * 100) if total_messages > 0 else 0
        
        # Time calculations (assuming 5-second intervals)
        driving_hours = (engine_on_count * 5) / 3600  # Convert to hours
        total_engine_hours = driving_hours  # Simplified
        
        # Idling time
        total_idling_seconds = sum(d.idling_time for d in telemetry_data)
        total_idling_hours = total_idling_seconds / 3600
        
        # Harsh events
        total_harsh_acceleration = sum(d.harsh_acceleration for d in telemetry_data)
        total_harsh_braking = sum(d.harsh_braking for d in telemetry_data)
        total_harsh_cornering = sum(d.harsh_cornering for d in telemetry_data)
        total_harsh_events = total_harsh_acceleration + total_harsh_braking + total_harsh_cornering
        
        # Speeding violations
        speeding_violations = sum(1 for d in telemetry_data if d.speed > 80)
        
        # Fuel analysis
        fuel_levels = [d.fuel_level for d in telemetry_data if d.fuel_level > 0]
        fuel_consumption = 0
        if len(fuel_levels) > 1:
            fuel_consumption = fuel_levels[0] - fuel_levels[-1]
        
        # CO2 emissions (rough estimate: 1L fuel = 2.31 kg CO2)
        co2_emission = fuel_consumption * 2.31
        
        # Eco driving score
        eco_driving_scores = [d.eco_driving_score for d in telemetry_data if d.eco_driving_score > 0]
        avg_eco_score = sum(eco_driving_scores) / len(eco_driving_scores) if eco_driving_scores else 0
        
        # Maintenance alerts
        all_maintenance_alerts = []
        for d in telemetry_data:
            all_maintenance_alerts.extend(d.maintenance_alerts)
        unique_alerts = list(set(all_maintenance_alerts))
        
        return {
            'totalDistance': total_distance,
            'maxSpeed': max_speed,
            'avgSpeed': avg_speed,
            'drivingHours': driving_hours,
            'totalEngineHours': total_engine_hours,
            'totalIdlingTime': total_idling_hours,
            'engineOnPercentage': engine_on_percentage,
            'totalHarshEvents': total_harsh_events,
            'harshAcceleration': total_harsh_acceleration,
            'harshBraking': total_harsh_braking,
            'harshCornering': total_harsh_cornering,
            'speedingViolations': speeding_violations,
            'fuelConsumption': fuel_consumption,
            'co2Emission': co2_emission,
            'avgEcoDrivingScore': avg_eco_score,
            'maintenanceAlerts': unique_alerts,
            'dataPoints': len(telemetry_data)
        }

    def assess_data_quality(self, telemetry_data):
        """Assess data quality for the unit"""
        if not telemetry_data:
            return {'overall_quality': 'Poor', 'issues': ['No data available']}
        
        total_records = len(telemetry_data)
        issues = []
        quality_score = 100
        
        # Check GPS data quality
        valid_gps = sum(1 for d in telemetry_data if d.latitude != 0 and d.longitude != 0)
        gps_completeness = (valid_gps / total_records) * 100
        if gps_completeness < 90:
            issues.append(f"GPS data only {gps_completeness:.1f}% complete")
            quality_score -= (100 - gps_completeness) * 0.5
        
        # Check speed data
        valid_speed = sum(1 for d in telemetry_data if d.speed >= 0)
        speed_completeness = (valid_speed / total_records) * 100
        if speed_completeness < 95:
            issues.append(f"Speed data only {speed_completeness:.1f}% complete")
            quality_score -= (100 - speed_completeness) * 0.3
        
        # Check for data gaps
        time_gaps = self.count_significant_time_gaps(telemetry_data)
        if time_gaps > 0:
            issues.append(f"{time_gaps} significant time gaps detected")
            quality_score -= time_gaps * 5
        
        # Check for anomalies
        anomalies = self.detect_data_anomalies(telemetry_data)
        if anomalies:
            issues.extend(anomalies)
            quality_score -= len(anomalies) * 10
        
        # Determine overall quality
        if quality_score >= 90:
            overall_quality = 'Excellent'
        elif quality_score >= 75:
            overall_quality = 'Good'
        elif quality_score >= 60:
            overall_quality = 'Fair'
        else:
            overall_quality = 'Poor'
        
        return {
            'overall_quality': overall_quality,
            'quality_score': max(0, quality_score),
            'gps_completeness': gps_completeness,
            'speed_completeness': speed_completeness,
            'time_gaps': time_gaps,
            'issues': issues
        }

    def count_significant_time_gaps(self, telemetry_data, threshold_minutes=10):
        """Count significant time gaps in data"""
        if len(telemetry_data) < 2:
            return 0
        
        gaps = 0
        for i in range(1, len(telemetry_data)):
            if telemetry_data[i].timestamp and telemetry_data[i-1].timestamp:
                time_diff = (telemetry_data[i].timestamp - telemetry_data[i-1].timestamp).total_seconds() / 60
                if time_diff > threshold_minutes:
                    gaps += 1
        
        return gaps

    def detect_data_anomalies(self, telemetry_data):
        """Detect data anomalies"""
        anomalies = []
        
        for data in telemetry_data:
            # Speed anomalies
            if data.speed < 0:
                anomalies.append("Negative speed detected")
                break
            if data.speed > 200:
                anomalies.append("Excessive speed detected (>200 km/h)")
                break
            
            # GPS anomalies
            if abs(data.latitude) > 90 or abs(data.longitude) > 180:
                anomalies.append("Invalid GPS coordinates")
                break
            
            # Power anomalies
            if data.power_voltage > 0 and data.power_voltage < 9000:  # Below 9V
                anomalies.append("Low power voltage detected")
                break
            
            # Fuel anomalies
            if data.fuel_level > 100:
                anomalies.append("Fuel level >100% detected")
                break
        
        return list(set(anomalies))  # Remove duplicates

    def calculate_fleet_summary(self, units_data):
        """Calculate fleet-wide summary metrics"""
        successful_units = [u for u in units_data if not u.get('error')]
        
        if not successful_units:
            return {}
        
        total_distance = sum(u['metrics'].get('totalDistance', 0) for u in successful_units)
        total_fuel = sum(u['metrics'].get('fuelConsumption', 0) for u in successful_units)
        total_harsh_events = sum(u['metrics'].get('totalHarshEvents', 0) for u in successful_units)
        total_driving_hours = sum(u['metrics'].get('drivingHours', 0) for u in successful_units)
        total_idling_hours = sum(u['metrics'].get('totalIdlingTime', 0) for u in successful_units)
        total_co2 = sum(u['metrics'].get('co2Emission', 0) for u in successful_units)
        
        avg_speed = sum(u['metrics'].get('avgSpeed', 0) for u in successful_units) / len(successful_units)
        max_speed_overall = max(u['metrics'].get('maxSpeed', 0) for u in successful_units)
        
        return {
            'total_units': len(successful_units),
            'total_distance_km': total_distance,
            'total_fuel_liters': total_fuel,
            'total_harsh_events': total_harsh_events,
            'total_driving_hours': total_driving_hours,
            'total_idling_hours': total_idling_hours,
            'total_co2_kg': total_co2,
            'avg_speed_kmh': avg_speed,
            'max_speed_kmh': max_speed_overall,
            'fuel_efficiency_km_per_liter': total_distance / total_fuel if total_fuel > 0 else 0,
            'avg_harsh_events_per_vehicle': total_harsh_events / len(successful_units),
            'fleet_utilization_percentage': (total_driving_hours / (len(successful_units) * 24)) * 100
        }

    def generate_fleet_data_quality_report(self, units_data):
        """Generate fleet-wide data quality report"""
        successful_units = [u for u in units_data if not u.get('error')]
        failed_units = [u for u in units_data if u.get('error')]
        
        quality_scores = [u['data_quality'].get('quality_score', 0) for u in successful_units]
        avg_quality = sum(quality_scores) / len(quality_scores) if quality_scores else 0
        
        all_issues = []
        for unit in successful_units:
            all_issues.extend(unit['data_quality'].get('issues', []))
        
        common_issues = {}
        for issue in all_issues:
            common_issues[issue] = common_issues.get(issue, 0) + 1
        
        return {
            'successful_extractions': len(successful_units),
            'failed_extractions': len(failed_units),
            'avg_data_quality_score': avg_quality,
            'units_with_excellent_quality': len([u for u in successful_units if u['data_quality'].get('overall_quality') == 'Excellent']),
            'units_with_poor_quality': len([u for u in successful_units if u['data_quality'].get('overall_quality') == 'Poor']),
            'common_issues': sorted(common_issues.items(), key=lambda x: x[1], reverse=True)[:5],
            'failed_units': [{'name': u['name'], 'error': u['error']} for u in failed_units]
        }

    def print_fleet_summary(self, fleet_data):
        """Print comprehensive fleet summary"""
        print(f"\n🎯 FLEET EXTRACTION SUMMARY")
        print("=" * 80)
        
        # Basic info
        info = fleet_data['extraction_info']
        print(f"📅 Date Range: {info['date_range']['from']} to {info['date_range']['to']}")
        print(f"📊 Report Type: {info['report_type']}")
        print(f"🚗 Total Units: {info['total_units']}")
        print(f"✅ Successful: {info['successful_units']}")
        print(f"❌ Failed: {info['failed_units']}")
        
        # Fleet summary
        summary = fleet_data['fleet_summary']
        if summary:
            print(f"\n📈 FLEET PERFORMANCE METRICS")
            print("-" * 50)
            print(f"🛣️  Total Distance: {summary['total_distance_km']:.2f} km")
            print(f"⛽ Total Fuel: {summary['total_fuel_liters']:.2f} L")
            print(f"🚨 Total Harsh Events: {summary['total_harsh_events']}")
            print(f"⏱️  Total Driving Hours: {summary['total_driving_hours']:.2f} h")
            print(f"💨 Average Speed: {summary['avg_speed_kmh']:.1f} km/h")
            print(f"📊 Fuel Efficiency: {summary['fuel_efficiency_km_per_liter']:.2f} km/L")
            print(f"🌍 Total CO2 Emission: {summary['total_co2_kg']:.2f} kg")
            print(f"📈 Fleet Utilization: {summary['fleet_utilization_percentage']:.1f}%")
        
        # Data quality
        quality = fleet_data['data_quality_report']
        print(f"\n✅ DATA QUALITY REPORT")
        print("-" * 50)
        print(f"📊 Average Quality Score: {quality['avg_data_quality_score']:.1f}%")
        print(f"🟢 Excellent Quality: {quality['units_with_excellent_quality']} units")
        print(f"🔴 Poor Quality: {quality['units_with_poor_quality']} units")
        
        if quality['common_issues']:
            print(f"⚠️  Common Issues:")
            for issue, count in quality['common_issues']:
                print(f"   • {issue}: {count} units")
        
        if quality['failed_units']:
            print(f"❌ Failed Units:")
            for unit in quality['failed_units']:
                print(f"   • {unit['name']}: {unit['error']}")
        
        # Excel report
        if 'excel_report' in fleet_data:
            print(f"\n📋 EXCEL REPORT GENERATED")
            print(f"   📄 File: {fleet_data['excel_report']}")
            print(f"   📊 Format: PTT Standard Template")
            print(f"   📈 Includes: Driver Performance, Vehicle Performance, Traffic Light Index")

# Example usage and main execution
def main():
    """Main execution function with enhanced features"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Enhanced Wialon Fleet Data Extractor")
    parser.add_argument('--token', type=str, required=True, help='Wialon API token')
    parser.add_argument('--start', type=str, required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', type=str, help='End date (YYYY-MM-DD)', default=None)
    parser.add_argument('--report-type', type=str, choices=['daily', 'weekly', 'monthly'], 
                       default='weekly', help='Report type')
    
    args = parser.parse_args()
    
    # Set up date range
    date_range = {
        'from': args.start,
        'to': args.end or args.start
    }
    
    # Initialize extractor
    extractor = EnhancedWialonExtractor(args.token)
    
    try:
        # Login
        extractor.login_sync()
        
        # Extract comprehensive fleet data
        fleet_data = extractor.extract_comprehensive_fleet_data(date_range, args.report_type)
        
        if fleet_data:
            print(f"\n🎉 EXTRACTION COMPLETED SUCCESSFULLY!")
            print(f"✅ Excel Report: {fleet_data.get('excel_report', 'N/A')}")
            print(f"✅ Data Quality: {fleet_data['data_quality_report']['avg_data_quality_score']:.1f}%")
            print(f"✅ Fleet Performance: {fleet_data['fleet_summary']['total_distance_km']:.2f} km total")
        else:
            print("❌ EXTRACTION FAILED")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        extractor.logout()

if __name__ == "__main__":
    # For testing without command line arguments
    TOKEN = "dd56d2bc9f2fa8a38a33b23cee3579c44B7EDE18BC70D5496297DA93724EAC95BF09624E"
    
    date_range = {
        'from': '2024-12-01',
        'to': '2024-12-07'
    }
    
    extractor = EnhancedWialonExtractor(TOKEN)
    
    try:
        extractor.login_sync()
        fleet_data = extractor.extract_comprehensive_fleet_data(date_range, "weekly")
        
        if fleet_data:
            print(f"\n🎉 SUCCESS! Generated: {fleet_data.get('excel_report', 'N/A')}")
    
    except Exception as e:
        print(f"❌ Error: {e}")
    
    finally:
        extractor.logout()