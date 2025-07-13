import React, { useState, useEffect, useRef } from 'react';
import { Download, Calendar, BarChart3, Users, Car, TrendingUp, AlertTriangle, Clock, Fuel, MapPin, Activity } from 'lucide-react';
import { LineChart, Line, AreaChart, Area, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';

// Enhanced Wialon API Service
class WialonService {
  constructor(token, baseUrl = "https://hst-api.wialon.com") {
    this.baseUrl = baseUrl;
    this.sessionId = null;
    this.token = token;
  }

  async login() {
    const response = await fetch(`${this.baseUrl}/wialon/ajax.html`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        svc: 'token/login',
        params: JSON.stringify({ token: this.token })
      })
    });
    
    const result = await response.json();
    if (result.error) throw new Error(`Login failed: ${result.error}`);
    
    this.sessionId = result.eid;
    return result;
  }

  async makeRequest(service, params = {}) {
    if (!this.sessionId) throw new Error("Not logged in");
    
    const response = await fetch(`${this.baseUrl}/wialon/ajax.html`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        svc: service,
        params: JSON.stringify(params),
        sid: this.sessionId
      })
    });
    
    const result = await response.json();
    if (result.error) throw new Error(`API Error: ${result.error}`);
    
    return result;
  }

  async getUnits() {
    return await this.makeRequest('core/search_items', {
      spec: {
        itemsType: 'avl_unit',
        propName: 'sys_name',
        propValueMask: '*',
        sortType: 'sys_name'
      },
      force: 1,
      flags: 0x00000001 | 0x00000002 | 0x00000008 | 0x00000020 | 0x00000200,
      from: 0,
      to: 100
    });
  }

  async getMessages(unitId, timeFrom, timeTo) {
    return await this.makeRequest('messages/load_interval', {
      itemId: unitId,
      timeFrom: timeFrom,
      timeTo: timeTo,
      flags: 0,
      flagsMask: 65535,
      loadCount: 5000
    });
  }

  async getTrips(unitId, timeFrom, timeTo) {
    return await this.makeRequest('report/exec_report', {
      reportResourceId: unitId,
      reportTemplateId: 1,
      reportTemplate: {
        n: 'trips',
        ct: 'avl_unit',
        p: {
          'grouping': '{"type":"day"}',
          'trips': '{"type":"all"}',
          'duration': 300,
          'filter': '{"type":"all"}'
        }
      },
      interval: { from: timeFrom, to: timeTo, flags: 0 }
    });
  }

  logout() {
    if (this.sessionId) {
      this.makeRequest('core/logout', {});
      this.sessionId = null;
    }
  }
}

// Data processing utilities
const processTelemetryData = (messages) => {
  if (!messages || !Array.isArray(messages)) return [];
  
  return messages.map(msg => {
    const pos = msg.pos || {};
    const params = msg.p || {};
    
    return {
      timestamp: new Date(msg.t * 1000),
      latitude: pos.y || 0,
      longitude: pos.x || 0,
      speed: pos.s || 0,
      course: pos.c || 0,
      altitude: pos.z || 0,
      satellites: pos.sc || 0,
      odometer: params.odometer || 0,
      engineOn: Boolean(params.engine_on || params.ignition),
      fuelLevel: params.fuel_level || 0,
      powerVoltage: params.power || 0,
      batteryVoltage: params.battery || 0,
      gsmSignal: params.gsm_signal || 0,
      temperature: params.pcb_temp || params.temperature || 0,
      harshAcceleration: params.harsh_acceleration || 0,
      harshBraking: params.harsh_braking || 0,
      harshCornering: params.harsh_cornering || 0,
      idlingTime: params.idling_time || 0,
      driverId: params.avl_driver || params.driver_code || '0',
      digitalInputs: Object.keys(params).filter(k => k.startsWith('din')).reduce((acc, k) => {
        acc[k] = Boolean(params[k]);
        return acc;
      }, {}),
      analogInputs: Object.keys(params).filter(k => k.startsWith('ain')).reduce((acc, k) => {
        acc[k] = params[k];
        return acc;
      }, {}),
      rawParams: params
    };
  });
};

const calculateMetrics = (telemetryData) => {
  if (!telemetryData.length) return {};
  
  const totalDistance = telemetryData.length > 1 ? 
    telemetryData[telemetryData.length - 1].odometer - telemetryData[0].odometer : 0;
  
  const speeds = telemetryData.map(d => d.speed).filter(s => s > 0);
  const maxSpeed = speeds.length ? Math.max(...speeds) : 0;
  const avgSpeed = speeds.length ? speeds.reduce((a, b) => a + b, 0) / speeds.length : 0;
  
  const engineOnTime = telemetryData.filter(d => d.engineOn).length;
  const totalTime = telemetryData.length;
  const engineOnPercentage = totalTime > 0 ? (engineOnTime / totalTime) * 100 : 0;
  
  const totalIdlingTime = telemetryData.reduce((sum, d) => sum + d.idlingTime, 0);
  const totalHarshEvents = telemetryData.reduce((sum, d) => 
    sum + d.harshAcceleration + d.harshBraking + d.harshCornering, 0);
  
  const speedingViolations = speeds.filter(s => s > 80).length;
  const fuelConsumption = telemetryData.length > 1 ?
    telemetryData[0].fuelLevel - telemetryData[telemetryData.length - 1].fuelLevel : 0;
  
  return {
    totalDistance: totalDistance / 1000, // Convert to km
    maxSpeed,
    avgSpeed,
    engineOnPercentage,
    totalIdlingTime: totalIdlingTime / 3600, // Convert to hours
    totalHarshEvents,
    speedingViolations,
    fuelConsumption,
    co2Emission: (fuelConsumption * 2.31), // Rough estimate
    drivingHours: (engineOnTime * 5) / 3600, // Assuming 5-second intervals
    totalEngineHours: (engineOnTime * 5) / 3600
  };
};

// Main Dashboard Component
const WialonDashboard = () => {
  const [isConnected, setIsConnected] = useState(false);
  const [units, setUnits] = useState([]);
  const [selectedUnits, setSelectedUnits] = useState([]);
  const [dateRange, setDateRange] = useState({
    from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0],
    to: new Date().toISOString().split('T')[0]
  });
  const [reportType, setReportType] = useState('daily');
  const [dashboardData, setDashboardData] = useState({});
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');
  const [token, setToken] = useState('dd56d2bc9f2fa8a38a33b23cee3579c44B7EDE18BC70D5496297DA93724EAC95BF09624E');
  
  const wialonService = useRef(null);

  // Initialize Wialon connection
  useEffect(() => {
    if (token) {
      wialonService.current = new WialonService(token);
    }
  }, [token]);

  const connectToWialon = async () => {
    setLoading(true);
    try {
      await wialonService.current.login();
      const unitsData = await wialonService.current.getUnits();
      setUnits(unitsData.items || []);
      setIsConnected(true);
      setSelectedUnits(unitsData.items?.slice(0, 5) || []); // Select first 5 units
    } catch (error) {
      console.error('Connection failed:', error);
      alert('Failed to connect to Wialon: ' + error.message);
    }
    setLoading(false);
  };

  const fetchDashboardData = async () => {
    if (!selectedUnits.length) return;
    
    setLoading(true);
    try {
      const fromTime = Math.floor(new Date(dateRange.from).getTime() / 1000);
      const toTime = Math.floor(new Date(dateRange.to).getTime() / 1000);
      
      const unitsData = await Promise.all(
        selectedUnits.map(async (unit) => {
          try {
            const messages = await wialonService.current.getMessages(unit.id, fromTime, toTime);
            const telemetryData = processTelemetryData(messages.messages || []);
            const metrics = calculateMetrics(telemetryData);
            
            return {
              id: unit.id,
              name: unit.nm,
              telemetryData,
              metrics,
              lastMessage: telemetryData[telemetryData.length - 1] || {}
            };
          } catch (error) {
            console.error(`Error fetching data for unit ${unit.nm}:`, error);
            return {
              id: unit.id,
              name: unit.nm,
              telemetryData: [],
              metrics: {},
              lastMessage: {},
              error: error.message
            };
          }
        })
      );
      
      setDashboardData({
        units: unitsData,
        summary: calculateSummaryMetrics(unitsData),
        chartData: prepareChartData(unitsData),
        lastUpdated: new Date()
      });
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      alert('Error fetching data: ' + error.message);
    }
    setLoading(false);
  };

  const calculateSummaryMetrics = (unitsData) => {
    const validUnits = unitsData.filter(unit => !unit.error);
    
    return {
      totalUnits: validUnits.length,
      totalDistance: validUnits.reduce((sum, unit) => sum + (unit.metrics.totalDistance || 0), 0),
      totalFuelConsumption: validUnits.reduce((sum, unit) => sum + (unit.metrics.fuelConsumption || 0), 0),
      totalHarshEvents: validUnits.reduce((sum, unit) => sum + (unit.metrics.totalHarshEvents || 0), 0),
      avgSpeed: validUnits.length ? validUnits.reduce((sum, unit) => sum + (unit.metrics.avgSpeed || 0), 0) / validUnits.length : 0,
      totalCO2: validUnits.reduce((sum, unit) => sum + (unit.metrics.co2Emission || 0), 0)
    };
  };

  const prepareChartData = (unitsData) => {
    const timeData = [];
    const speedData = [];
    const fuelData = [];
    
    unitsData.forEach(unit => {
      unit.telemetryData.forEach((data, index) => {
        if (index % 10 === 0) { // Sample every 10th point for performance
          const time = data.timestamp.toLocaleTimeString();
          timeData.push({
            time,
            unit: unit.name,
            speed: data.speed,
            fuel: data.fuelLevel,
            engineOn: data.engineOn ? 1 : 0
          });
        }
      });
    });
    
    return {
      timeData: timeData.slice(-50), // Last 50 points
      unitMetrics: unitsData.map(unit => ({
        name: unit.name,
        distance: unit.metrics.totalDistance || 0,
        fuel: unit.metrics.fuelConsumption || 0,
        harshEvents: unit.metrics.totalHarshEvents || 0,
        co2: unit.metrics.co2Emission || 0
      }))
    };
  };

  const generateExcelReport = () => {
    if (!dashboardData.units) return;
    
    // Create report data matching the template structure
    const reportData = {
      driverReport: generateDriverReport(),
      vehicleReport: generateVehicleReport()
    };
    
    // Generate Excel file (simplified version)
    const csvContent = generateCSVContent(reportData);
    downloadCSV(csvContent, `PTT_Report_${dateRange.from}_${dateRange.to}.csv`);
  };

  const generateDriverReport = () => {
    return dashboardData.units.map(unit => ({
      driverName: `Driver for ${unit.name}`,
      totalDistance: unit.metrics.totalDistance || 0,
      drivingHours: unit.metrics.drivingHours || 0,
      idlingDuration: unit.metrics.totalIdlingTime || 0,
      engineHours: unit.metrics.totalEngineHours || 0,
      speedingViolations: unit.metrics.speedingViolations || 0,
      harshAcceleration: unit.telemetryData.reduce((sum, d) => sum + d.harshAcceleration, 0),
      harshBraking: unit.telemetryData.reduce((sum, d) => sum + d.harshBraking, 0),
      harshTurning: unit.telemetryData.reduce((sum, d) => sum + d.harshCornering, 0)
    }));
  };

  const generateVehicleReport = () => {
    return dashboardData.units.map(unit => ({
      vehicleNo: unit.name,
      department: 'PTT TANKER',
      totalDistance: unit.metrics.totalDistance || 0,
      drivingHours: unit.metrics.drivingHours || 0,
      idlingDuration: unit.metrics.totalIdlingTime || 0,
      engineHours: unit.metrics.totalEngineHours || 0,
      speedingViolations: unit.metrics.speedingViolations || 0,
      harshAcceleration: unit.telemetryData.reduce((sum, d) => sum + d.harshAcceleration, 0),
      harshBraking: unit.telemetryData.reduce((sum, d) => sum + d.harshBraking, 0),
      harshTurning: unit.telemetryData.reduce((sum, d) => sum + d.harshCornering, 0),
      fuelConsumption: unit.metrics.fuelConsumption || 0,
      co2Emission: unit.metrics.co2Emission || 0
    }));
  };

  const generateCSVContent = (reportData) => {
    let csv = "PTT Fleet Management Report\n\n";
    csv += `Report Period: ${dateRange.from} to ${dateRange.to}\n\n`;
    
    // Vehicle Report
    csv += "VEHICLE PERFORMANCE SUMMARY\n";
    csv += "Department,Vehicle No.,Total Distance(KM),Driving Hours,Idling Duration,Engine Hours,Speeding Violations,Harsh Acceleration,Harsh Braking,Harsh Turning,Fuel Consumption(L),CO2 Emission(KG)\n";
    
    reportData.vehicleReport.forEach(vehicle => {
      csv += `${vehicle.department},${vehicle.vehicleNo},${vehicle.totalDistance.toFixed(2)},${vehicle.drivingHours.toFixed(2)},${vehicle.idlingDuration.toFixed(2)},${vehicle.engineHours.toFixed(2)},${vehicle.speedingViolations},${vehicle.harshAcceleration},${vehicle.harshBraking},${vehicle.harshTurning},${vehicle.fuelConsumption.toFixed(2)},${vehicle.co2Emission.toFixed(2)}\n`;
    });
    
    csv += "\n\nDRIVER PERFORMANCE SUMMARY\n";
    csv += "Driver Name,Total Distance(KM),Driving Hours,Idling Duration,Engine Hours,Speeding Violations,Harsh Acceleration,Harsh Braking,Harsh Turning\n";
    
    reportData.driverReport.forEach(driver => {
      csv += `${driver.driverName},${driver.totalDistance.toFixed(2)},${driver.drivingHours.toFixed(2)},${driver.idlingDuration.toFixed(2)},${driver.engineHours.toFixed(2)},${driver.speedingViolations},${driver.harshAcceleration},${driver.harshBraking},${driver.harshTurning}\n`;
    });
    
    return csv;
  };

  const downloadCSV = (content, filename) => {
    const blob = new Blob([content], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
  };

  const COLORS = ['#0088FE', '#00C49F', '#FFBB28', '#FF8042', '#8884D8'];

  return (
    <div className="min-h-screen bg-gray-50 p-4">
      <div className="max-w-7xl mx-auto">
        {/* Header */}
        <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-3xl font-bold text-gray-900">PTT Fleet Management System</h1>
              <p className="text-gray-600 mt-1">Real-time vehicle tracking and performance monitoring</p>
            </div>
            <div className="flex items-center gap-4">
              <div className={`px-3 py-1 rounded-full text-sm font-medium ${isConnected ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                {isConnected ? '🟢 Connected' : '🔴 Disconnected'}
              </div>
              {dashboardData.lastUpdated && (
                <div className="text-sm text-gray-500">
                  Last updated: {dashboardData.lastUpdated.toLocaleTimeString()}
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Connection Setup */}
        {!isConnected && (
          <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
            <h2 className="text-xl font-semibold mb-4">Connect to Wialon</h2>
            <div className="flex gap-4 items-end">
              <div className="flex-1">
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  API Token
                </label>
                <input
                  type="text"
                  value={token}
                  onChange={(e) => setToken(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="Enter your Wialon API token"
                />
              </div>
              <button
                onClick={connectToWialon}
                disabled={loading || !token}
                className="px-6 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Connecting...' : 'Connect'}
              </button>
            </div>
          </div>
        )}

        {/* Controls */}
        {isConnected && (
          <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  <Calendar className="w-4 h-4 inline mr-1" />
                  From Date
                </label>
                <input
                  type="date"
                  value={dateRange.from}
                  onChange={(e) => setDateRange(prev => ({ ...prev, from: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  <Calendar className="w-4 h-4 inline mr-1" />
                  To Date
                </label>
                <input
                  type="date"
                  value={dateRange.to}
                  onChange={(e) => setDateRange(prev => ({ ...prev, to: e.target.value }))}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-2">
                  Report Type
                </label>
                <select
                  value={reportType}
                  onChange={(e) => setReportType(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                >
                  <option value="daily">Daily</option>
                  <option value="weekly">Weekly</option>
                  <option value="monthly">Monthly</option>
                </select>
              </div>
              <div className="flex gap-2">
                <button
                  onClick={fetchDashboardData}
                  disabled={loading}
                  className="flex-1 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50"
                >
                  {loading ? 'Loading...' : 'Refresh Data'}
                </button>
                <button
                  onClick={generateExcelReport}
                  disabled={!dashboardData.units || loading}
                  className="px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700 disabled:opacity-50"
                >
                  <Download className="w-4 h-4" />
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Unit Selection */}
        {units.length > 0 && (
          <div className="bg-white rounded-lg shadow-sm p-6 mb-6">
            <h3 className="text-lg font-semibold mb-4">Select Vehicles ({selectedUnits.length} selected)</h3>
            <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-6 gap-3">
              {units.map(unit => (
                <label key={unit.id} className="flex items-center space-x-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={selectedUnits.some(u => u.id === unit.id)}
                    onChange={(e) => {
                      if (e.target.checked) {
                        setSelectedUnits(prev => [...prev, unit]);
                      } else {
                        setSelectedUnits(prev => prev.filter(u => u.id !== unit.id));
                      }
                    }}
                    className="w-4 h-4 text-blue-600 border-gray-300 rounded focus:ring-blue-500"
                  />
                  <span className="text-sm text-gray-700 truncate">{unit.nm}</span>
                </label>
              ))}
            </div>
          </div>
        )}

        {/* Dashboard Content */}
        {dashboardData.summary && (
          <>
            {/* KPI Cards */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-6 gap-4 mb-6">
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <Car className="w-8 h-8 text-blue-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">Total Vehicles</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.totalUnits}</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <MapPin className="w-8 h-8 text-green-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">Total Distance</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.totalDistance.toFixed(1)} km</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <Fuel className="w-8 h-8 text-yellow-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">Fuel Consumed</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.totalFuelConsumption.toFixed(1)} L</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <TrendingUp className="w-8 h-8 text-blue-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">Avg Speed</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.avgSpeed.toFixed(1)} km/h</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <AlertTriangle className="w-8 h-8 text-red-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">Harsh Events</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.totalHarshEvents}</p>
                  </div>
                </div>
              </div>
              
              <div className="bg-white rounded-lg shadow-sm p-6">
                <div className="flex items-center">
                  <Activity className="w-8 h-8 text-purple-600" />
                  <div className="ml-4">
                    <p className="text-sm font-medium text-gray-600">CO2 Emission</p>
                    <p className="text-2xl font-bold text-gray-900">{dashboardData.summary.totalCO2.toFixed(1)} kg</p>
                  </div>
                </div>
              </div>
            </div>

            {/* Tabs */}
            <div className="bg-white rounded-lg shadow-sm mb-6">
              <div className="border-b border-gray-200">
                <nav className="-mb-px flex space-x-8 px-6">
                  {[
                    { id: 'overview', name: 'Overview', icon: BarChart3 },
                    { id: 'vehicles', name: 'Vehicle Performance', icon: Car },
                    { id: 'drivers', name: 'Driver Performance', icon: Users },
                    { id: 'realtime', name: 'Real-time', icon: Activity }
                  ].map((tab) => (
                    <button
                      key={tab.id}
                      onClick={() => setActiveTab(tab.id)}
                      className={`py-4 px-1 border-b-2 font-medium text-sm flex items-center gap-2 ${
                        activeTab === tab.id
                          ? 'border-blue-500 text-blue-600'
                          : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                      }`}
                    >
                      <tab.icon className="w-4 h-4" />
                      {tab.name}
                    </button>
                  ))}
                </nav>
              </div>

              <div className="p-6">
                {activeTab === 'overview' && (
                  <div className="space-y-6">
                    {/* Charts Row */}
                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                      {/* Speed Chart */}
                      <div className="bg-gray-50 rounded-lg p-4">
                        <h3 className="text-lg font-semibold mb-4">Speed Trends</h3>
                        <ResponsiveContainer width="100%" height={300}>
                          <LineChart data={dashboardData.chartData?.timeData || []}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="time" />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Line type="monotone" dataKey="speed" stroke="#8884d8" strokeWidth={2} />
                          </LineChart>
                        </ResponsiveContainer>
                      </div>

                      {/* Vehicle Metrics */}
                      <div className="bg-gray-50 rounded-lg p-4">
                        <h3 className="text-lg font-semibold mb-4">Vehicle Performance Comparison</h3>
                        <ResponsiveContainer width="100%" height={300}>
                          <BarChart data={dashboardData.chartData?.unitMetrics || []}>
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="name" />
                            <YAxis />
                            <Tooltip />
                            <Legend />
                            <Bar dataKey="distance" fill="#8884d8" name="Distance (km)" />
                            <Bar dataKey="fuel" fill="#82ca9d" name="Fuel (L)" />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </div>

                    {/* Harsh Events Chart */}
                    <div className="bg-gray-50 rounded-lg p-4">
                      <h3 className="text-lg font-semibold mb-4">Harsh Events by Vehicle</h3>
                      <ResponsiveContainer width="100%" height={300}>
                        <BarChart data={dashboardData.chartData?.unitMetrics || []}>
                          <CartesianGrid strokeDasharray="3 3" />
                          <XAxis dataKey="name" />
                          <YAxis />
                          <Tooltip />
                          <Legend />
                          <Bar dataKey="harshEvents" fill="#ff7300" name="Harsh Events" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                )}

                {activeTab === 'vehicles' && (
                  <div className="space-y-6">
                    <h3 className="text-xl font-semibold">Vehicle Performance Summary</h3>
                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Vehicle No.</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Distance (km)</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Driving Hours</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Idling Hours</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Max Speed</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Harsh Events</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Fuel (L)</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">CO2 (kg)</th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {dashboardData.units?.map((unit, index) => (
                            <tr key={unit.id} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                              <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                {unit.name}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.totalDistance || 0).toFixed(2)}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.drivingHours || 0).toFixed(2)}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.totalIdlingTime || 0).toFixed(2)}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.maxSpeed || 0).toFixed(1)} km/h
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                <span className={`px-2 py-1 rounded-full text-xs ${
                                  (unit.metrics.totalHarshEvents || 0) > 10 
                                    ? 'bg-red-100 text-red-800' 
                                    : (unit.metrics.totalHarshEvents || 0) > 5 
                                    ? 'bg-yellow-100 text-yellow-800' 
                                    : 'bg-green-100 text-green-800'
                                }`}>
                                  {unit.metrics.totalHarshEvents || 0}
                                </span>
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.fuelConsumption || 0).toFixed(2)}
                              </td>
                              <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                {(unit.metrics.co2Emission || 0).toFixed(2)}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {activeTab === 'drivers' && (
                  <div className="space-y-6">
                    <h3 className="text-xl font-semibold">Driver Performance Summary</h3>
                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Driver Assignment</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Vehicle</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Distance (km)</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Driving Hours</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Speeding Violations</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Harsh Acceleration</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Harsh Braking</th>
                            <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Harsh Turning</th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {dashboardData.units?.map((unit, index) => {
                            const driverData = unit.telemetryData.reduce((acc, data) => {
                              acc.harshAcceleration += data.harshAcceleration;
                              acc.harshBraking += data.harshBraking;
                              acc.harshCornering += data.harshCornering;
                              return acc;
                            }, { harshAcceleration: 0, harshBraking: 0, harshCornering: 0 });

                            return (
                              <tr key={unit.id} className={index % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                                <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                                  PTT TANKER DRIVERS
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {unit.name}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {(unit.metrics.totalDistance || 0).toFixed(2)}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {(unit.metrics.drivingHours || 0).toFixed(2)}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  <span className={`px-2 py-1 rounded-full text-xs ${
                                    (unit.metrics.speedingViolations || 0) > 5 
                                      ? 'bg-red-100 text-red-800' 
                                      : (unit.metrics.speedingViolations || 0) > 2 
                                      ? 'bg-yellow-100 text-yellow-800' 
                                      : 'bg-green-100 text-green-800'
                                  }`}>
                                    {unit.metrics.speedingViolations || 0}
                                  </span>
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {driverData.harshAcceleration}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {driverData.harshBraking}
                                </td>
                                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                  {driverData.harshCornering}
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {activeTab === 'realtime' && (
                  <div className="space-y-6">
                    <div className="flex justify-between items-center">
                      <h3 className="text-xl font-semibold">Real-time Vehicle Status</h3>
                      <button
                        onClick={fetchDashboardData}
                        className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
                      >
                        Refresh
                      </button>
                    </div>
                    
                    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                      {dashboardData.units?.map((unit) => (
                        <div key={unit.id} className="bg-white border rounded-lg p-4 shadow-sm">
                          <div className="flex justify-between items-start mb-3">
                            <h4 className="font-semibold text-gray-900">{unit.name}</h4>
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${
                              unit.lastMessage.engineOn 
                                ? 'bg-green-100 text-green-800' 
                                : 'bg-gray-100 text-gray-800'
                            }`}>
                              {unit.lastMessage.engineOn ? 'Engine ON' : 'Engine OFF'}
                            </span>
                          </div>
                          
                          <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span className="text-gray-600">Speed:</span>
                              <span className="font-medium">{(unit.lastMessage.speed || 0).toFixed(1)} km/h</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Location:</span>
                              <span className="font-medium text-xs">
                                {unit.lastMessage.latitude?.toFixed(4)}, {unit.lastMessage.longitude?.toFixed(4)}
                              </span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Fuel:</span>
                              <span className="font-medium">{(unit.lastMessage.fuelLevel || 0).toFixed(1)}%</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">GSM Signal:</span>
                              <span className="font-medium">{unit.lastMessage.gsmSignal || 0}</span>
                            </div>
                            <div className="flex justify-between">
                              <span className="text-gray-600">Last Update:</span>
                              <span className="font-medium text-xs">
                                {unit.lastMessage.timestamp ? 
                                  unit.lastMessage.timestamp.toLocaleTimeString() : 
                                  'No data'
                                }
                              </span>
                            </div>
                          </div>
                          
                          {unit.error && (
                            <div className="mt-3 p-2 bg-red-50 border border-red-200 rounded text-red-700 text-xs">
                              Error: {unit.error}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          </>
        )}

        {/* Loading State */}
        {loading && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 flex items-center space-x-4">
              <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
              <span className="text-gray-700">Loading data from Wialon...</span>
            </div>
          </div>
        )}

        {/* Footer */}
        <div className="bg-white rounded-lg shadow-sm p-4 mt-6">
          <div className="text-center text-sm text-gray-500">
            PTT Fleet Management System - Real-time vehicle tracking and performance monitoring
            {dashboardData.lastUpdated && (
              <span className="block mt-1">
                Data last refreshed: {dashboardData.lastUpdated.toLocaleString()}
              </span>
            )}
          </div>
        </div>
      </div>
    </div>
  );
};

export default WialonDashboard;