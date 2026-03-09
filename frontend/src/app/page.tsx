"use client";

import { useEffect, useState } from "react";
import { format } from "date-fns";
import { AlertTriangle, Activity, CheckCircle2, AlertCircle, RefreshCw, Database, Server, BrainCircuit, ActivitySquare, ChevronDown, ChevronUp, Search, Filter, Github } from "lucide-react";

interface SepsisAlert {
  subject_id: string;
  charttime: string;
  risk_level: "HIGH" | "MEDIUM" | "LOW";
  confidence: number;
  primary_concern: string;
  reasoning: string;
  recommended_action: string;
  heart_rate_mean: number | null;
  map_mean: number | null;
  spo2_min: number | null;
  lactate: number | null;
  sofa_proxy: number | null;
}

interface DashboardStats {
  total_patients: number;
  high_risk: number;
  medium_risk: number;
  low_risk: number;
}

export default function Dashboard() {
  const [patients, setPatients] = useState<SepsisAlert[]>([]);
  const [stats, setStats] = useState<DashboardStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [showArchitecture, setShowArchitecture] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [filterRisk, setFilterRisk] = useState<"ALL" | "HIGH" | "MEDIUM" | "LOW">("ALL");

  const fetchData = async () => {
    try {
      setLoading(true);
      setError("");
      
      const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';
      const [patientsRes, statsRes] = await Promise.all([
        fetch(`${API_URL}/api/patients`),
        fetch(`${API_URL}/api/stats`)
      ]);
      
      if (!patientsRes.ok || !statsRes.ok) {
        throw new Error("Failed to fetch data from API");
      }
      
      const patientsData = await patientsRes.json();
      const statsData = await statsRes.json();
      
      setPatients(patientsData.data);
      setStats(statsData);
    } catch (err) {
      console.error(err);
      setError("Could not connect to the Sepsis Risk API. Is the backend running?");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
    // Auto refresh every 30 seconds
    const interval = setInterval(fetchData, 30000);
    return () => clearInterval(interval);
  }, []);

  const filteredPatients = patients.filter(p => {
    const matchesSearch = p.subject_id.includes(searchQuery);
    const matchesRisk = filterRisk === "ALL" || p.risk_level === filterRisk;
    return matchesSearch && matchesRisk;
  });

  const getRiskColor = (risk: string) => {
    switch (risk) {
      case "HIGH": return "bg-red-500/10 border-red-500/50 text-red-500";
      case "MEDIUM": return "bg-yellow-500/10 border-yellow-500/50 text-yellow-500";
      case "LOW": return "bg-green-500/10 border-green-500/50 text-green-500";
      default: return "bg-gray-500/10 border-gray-500/50 text-gray-400";
    }
  };

  const getRiskIcon = (risk: string) => {
    switch (risk) {
      case "HIGH": return <AlertTriangle className="h-5 w-5 text-red-500" />;
      case "MEDIUM": return <AlertCircle className="h-5 w-5 text-yellow-500" />;
      case "LOW": return <CheckCircle2 className="h-5 w-5 text-green-500" />;
      default: return <Activity className="h-5 w-5 text-gray-400" />;
    }
  };

  return (
    <div className="min-h-screen bg-neutral-950 text-neutral-100 p-6 md:p-12 font-sans selection:bg-blue-500/30">
      <div className="max-w-7xl mx-auto space-y-8">
        
        {/* Header */}
        <header className="flex flex-col md:flex-row md:items-end justify-between gap-4 border-b border-neutral-800 pb-6">
          <div>
            <div className="flex items-center gap-3 mb-2">
              <div className="h-10 w-10 rounded-xl bg-blue-500/20 flex items-center justify-center border border-blue-500/30">
                <Activity className="h-6 w-6 text-blue-400" />
              </div>
              <h1 className="text-3xl font-bold tracking-tight text-white">Sepsis Watchtower</h1>
            </div>
            <p className="text-neutral-400">Agentic AI real-time risk stratification</p>
          </div>
          <div className="flex items-center gap-3">
            <a 
              href="https://github.com/leela56/sepsis-risk-dashboard"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 px-4 py-2 bg-neutral-900 hover:bg-neutral-800 border border-neutral-800 rounded-lg text-sm text-neutral-300 transition-colors"
            >
              <Github className="h-4 w-4" />
              Source Code
            </a>
            <button 
              onClick={fetchData}
              disabled={loading}
              className="flex items-center gap-2 px-4 py-2 bg-blue-600/10 hover:bg-blue-600/20 border border-blue-500/30 rounded-lg text-sm text-blue-400 transition-colors disabled:opacity-50"
            >
              <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
              Refresh Data
            </button>
          </div>
        </header>

        {error && (
          <div className="bg-red-500/10 border border-red-500/50 rounded-xl p-4 flex items-start gap-3 text-red-400">
            <AlertTriangle className="h-5 w-5 mt-0.5 shrink-0" />
            <p>{error}</p>
          </div>
        )}

        {/* Architecture Showcase Toggle */}
        <div className="bg-neutral-900 border border-neutral-800 rounded-2xl overflow-hidden transition-all duration-300">
          <button 
            onClick={() => setShowArchitecture(!showArchitecture)}
            className="w-full px-6 py-4 flex items-center justify-between text-left hover:bg-neutral-800/50 transition-colors"
          >
            <div className="flex items-center gap-3">
              <BrainCircuit className="h-5 w-5 text-purple-400" />
              <span className="font-semibold text-neutral-200">How this AI Agent works (Architecture Flow)</span>
            </div>
            {showArchitecture ? <ChevronUp className="h-5 w-5 text-neutral-500" /> : <ChevronDown className="h-5 w-5 text-neutral-500" />}
          </button>
          
          {showArchitecture && (
            <div className="px-6 pb-6 pt-2 border-t border-neutral-800/50">
              <div className="grid md:grid-cols-4 gap-4 mt-4 relative">
                {/* Connecting Line */}
                <div className="hidden md:block absolute top-8 left-[10%] right-[10%] h-0.5 bg-gradient-to-r from-blue-500/20 via-purple-500/20 to-green-500/20 z-0"></div>
                
                <div className="group cursor-help relative z-10 flex flex-col items-center text-center space-y-3 bg-neutral-950 p-4 rounded-xl border border-neutral-800/50 shadow-xl hover:border-blue-500/50 transition-colors">
                  <div className="h-12 w-12 rounded-full bg-blue-500/10 border border-blue-500/30 flex items-center justify-center">
                    <ActivitySquare className="h-6 w-6 text-blue-400" />
                  </div>
                  <h3 className="font-bold text-neutral-200">1. Data Stream</h3>
                  <p className="text-xs text-neutral-400">MIMIC-IV vital signs and labs are streamed in real-time into <span className="text-blue-300">Confluent Kafka</span>.</p>
                  
                  {/* Tooltip */}
                  <div className="absolute top-full mt-3 w-64 p-3 bg-neutral-800 border border-neutral-700 rounded-lg shadow-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 text-left">
                    <p className="text-xs text-neutral-300"><strong className="text-white">Why Kafka?</strong> High-throughput event streaming mimics an actual hospital bedside HL7 feed where millisecond delays matter.</p>
                  </div>
                </div>

                <div className="group cursor-help relative z-10 flex flex-col items-center text-center space-y-3 bg-neutral-950 p-4 rounded-xl border border-neutral-800/50 shadow-xl hover:border-indigo-500/50 transition-colors">
                  <div className="h-12 w-12 rounded-full bg-indigo-500/10 border border-indigo-500/30 flex items-center justify-center">
                    <Server className="h-6 w-6 text-indigo-400" />
                  </div>
                  <h3 className="font-bold text-neutral-200">2. Feature Engine</h3>
                  <p className="text-xs text-neutral-400">Python script computes rolling hourly averages, trends, and proxies like <span className="text-indigo-300">SOFA score</span>.</p>
                  
                  {/* Tooltip */}
                  <div className="absolute top-full mt-3 w-64 p-3 bg-neutral-800 border border-neutral-700 rounded-lg shadow-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 text-left">
                    <p className="text-xs text-neutral-300"><strong className="text-white">Why Pandas?</strong> Raw signals are noisy. We compute 1-hour and 3-hour rolling aggregations so the AI can analyze medical state trends, not just point-in-time dots.</p>
                  </div>
                </div>

                <div className="group cursor-help relative z-10 flex flex-col items-center text-center space-y-3 bg-neutral-950 p-4 rounded-xl border border-purple-500/30 shadow-[0_0_15px_rgba(168,85,247,0.1)] hover:border-purple-400/60 transition-colors">
                  <div className="h-12 w-12 rounded-full bg-purple-500/10 border border-purple-500/30 flex items-center justify-center">
                    <BrainCircuit className="h-6 w-6 text-purple-400" />
                  </div>
                  <h3 className="font-bold text-white">3. Claude AI Agent</h3>
                  <p className="text-xs text-neutral-400">Anthropic Claude assesses rolling data with a structured prompt, returning JSON <span className="text-purple-300">risk levels & clinical reasoning</span>.</p>
                  
                  {/* Tooltip */}
                  <div className="absolute top-full mt-3 w-64 p-3 bg-neutral-800 border border-neutral-700 rounded-lg shadow-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 text-left">
                    <p className="text-xs text-neutral-300"><strong className="text-white">Why Claude 3.5 Sonnet?</strong> We use Sonnet for its superior adherence to structured JSON output and deep clinical reasoning capabilities to reduce false-positive alarm fatigue.</p>
                  </div>
                </div>

                <div className="group cursor-help relative z-10 flex flex-col items-center text-center space-y-3 bg-neutral-950 p-4 rounded-xl border border-green-500/20 shadow-xl hover:border-green-500/50 transition-colors">
                  <div className="h-12 w-12 rounded-full bg-green-500/10 border border-green-500/30 flex items-center justify-center">
                    <Database className="h-6 w-6 text-green-400" />
                  </div>
                  <h3 className="font-bold text-neutral-200">4. Live Delivery</h3>
                  <p className="text-xs text-neutral-400">Assessments hit <span className="text-green-300">Supabase</span> (PostgreSQL), served by <span className="text-emerald-300">FastAPI</span> to this Next.js dashboard.</p>
                  
                  {/* Tooltip */}
                  <div className="absolute top-full mt-3 w-64 p-3 bg-neutral-800 border border-neutral-700 rounded-lg shadow-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-200 pointer-events-none z-50 text-left md:-left-1/2">
                    <p className="text-xs text-neutral-300"><strong className="text-white">Why Supabase & FastAPI?</strong> We store the structured assessments persistently in Postgres so the high-performance async FastAPI backend can serve instant real-time data to doctors.</p>
                  </div>
                </div>
              </div>
            </div>
          )}
        </div>

        {/* Stats Row */}
        {stats && (
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="bg-neutral-900 border border-neutral-800 rounded-2xl p-6">
              <p className="text-neutral-400 text-sm font-medium mb-1">Monitored Patients</p>
              <p className="text-3xl font-bold text-white">{stats.total_patients}</p>
            </div>
            <div className="bg-red-950/20 border border-red-900/50 rounded-2xl p-6 relative overflow-hidden">
              <div className="absolute top-0 right-0 p-4 opacity-10"><AlertTriangle className="h-16 w-16 text-red-500" /></div>
              <p className="text-red-400/80 text-sm font-medium mb-1">Critical Alarms (High Risk)</p>
              <p className="text-3xl font-bold text-red-400">{stats.high_risk}</p>
            </div>
            <div className="bg-yellow-950/20 border border-yellow-900/50 rounded-2xl p-6 relative overflow-hidden">
              <div className="absolute top-0 right-0 p-4 opacity-10"><AlertCircle className="h-16 w-16 text-yellow-500" /></div>
              <p className="text-yellow-400/80 text-sm font-medium mb-1">Warning (Medium Risk)</p>
              <p className="text-3xl font-bold text-yellow-400">{stats.medium_risk}</p>
            </div>
            <div className="bg-green-950/20 border border-green-900/50 rounded-2xl p-6 relative overflow-hidden">
              <div className="absolute top-0 right-0 p-4 opacity-10"><CheckCircle2 className="h-16 w-16 text-green-500" /></div>
              <p className="text-green-400/80 text-sm font-medium mb-1">Stable (Low Risk)</p>
              <p className="text-3xl font-bold text-green-400">{stats.low_risk}</p>
            </div>
          </div>
        )}

        {/* Patient Grid & Filters */}
        <div className="space-y-6">
          <div className="flex flex-col md:flex-row md:items-center justify-between gap-4">
            <h2 className="text-xl font-semibold text-white flex items-center gap-2">
              Live Patient Assessments
              <span className="text-xs font-normal px-2 py-0.5 rounded-full bg-neutral-800 text-neutral-400 border border-neutral-700">
                {filteredPatients.length} active
              </span>
            </h2>

            {/* Filter Controls */}
            <div className="flex flex-col sm:flex-row gap-3">
              <div className="relative">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-neutral-500" />
                <input 
                  type="text" 
                  placeholder="Search Patient ID..."
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  className="pl-9 pr-4 py-2 bg-neutral-900 border border-neutral-800 rounded-lg text-sm text-white focus:outline-none focus:border-blue-500/50 w-full sm:w-48 placeholder:text-neutral-600 transition-colors"
                />
              </div>
              
              <div className="flex items-center bg-neutral-900 border border-neutral-800 rounded-lg p-1">
                {(["ALL", "HIGH", "MEDIUM", "LOW"] as const).map(risk => (
                  <button
                    key={risk}
                    onClick={() => setFilterRisk(risk)}
                    className={`px-3 py-1.5 text-xs font-medium rounded-md transition-colors ${
                      filterRisk === risk 
                        ? risk === 'HIGH' ? 'bg-red-500/20 text-red-400'
                        : risk === 'MEDIUM' ? 'bg-yellow-500/20 text-yellow-400'
                        : risk === 'LOW' ? 'bg-green-500/20 text-green-400'
                        : 'bg-neutral-800 text-white'
                        : 'text-neutral-500 hover:text-neutral-300'
                    }`}
                  >
                    {risk}
                  </button>
                ))}
              </div>
            </div>
          </div>

          {loading && patients.length === 0 ? (
            <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-6 animate-pulse">
              {[1, 2, 3, 4, 5, 6].map(i => (
                <div key={i} className="h-64 bg-neutral-900 rounded-2xl border border-neutral-800"></div>
              ))}
            </div>
          ) : filteredPatients.length === 0 ? (
            <div className="text-center py-12 border border-neutral-800 border-dashed rounded-2xl bg-neutral-900/50">
              <Filter className="h-8 w-8 text-neutral-600 mx-auto mb-3" />
              <p className="text-neutral-400 font-medium">No patients found matching your filters.</p>
              <button onClick={() => {setSearchQuery(""); setFilterRisk("ALL");}} className="mt-2 text-sm text-blue-400 hover:text-blue-300">Clear filters</button>
            </div>
          ) : (
            <div className="grid md:grid-cols-2 xl:grid-cols-3 gap-6">
              {filteredPatients.map((patient) => (
                <div 
                  key={patient.subject_id} 
                  className={`bg-neutral-900 border rounded-2xl p-5 flex flex-col hover:border-neutral-600 transition-colors ${
                    patient.risk_level === 'HIGH' ? 'border-red-900/50 shadow-[0_0_15px_rgba(239,68,68,0.05)]' : 
                    patient.risk_level === 'MEDIUM' ? 'border-yellow-900/50' : 'border-neutral-800'
                  }`}
                >
                  {/* Card Header */}
                  <div className="flex justify-between items-start mb-4">
                    <div>
                      <h3 className="text-lg font-bold text-white flex items-center gap-2">
                        ID: {patient.subject_id}
                      </h3>
                      <p className="text-xs text-neutral-500 mt-1">
                        Assessed: {format(new Date(patient.charttime), "MMM d, HH:mm")}
                      </p>
                    </div>
                    <div className={`flex items-center gap-1.5 px-3 py-1 rounded-full border text-xs font-bold tracking-wide ${getRiskColor(patient.risk_level)}`}>
                      {getRiskIcon(patient.risk_level)}
                      {patient.risk_level}
                    </div>
                  </div>

                  {/* Vitals Mini-Grid */}
                  <div className="grid grid-cols-4 gap-2 mb-4 bg-neutral-950/50 rounded-xl p-3 border border-neutral-800/50">
                    <div className="text-center group">
                      <p className="text-[10px] uppercase tracking-wider text-neutral-500 mb-0.5">HR</p>
                      <p className={`font-mono font-medium ${patient.heart_rate_mean && patient.heart_rate_mean > 90 ? 'text-red-400' : 'text-neutral-300'}`}>
                        {patient.heart_rate_mean ? Math.round(patient.heart_rate_mean) : '-'}
                      </p>
                    </div>
                    <div className="text-center group border-l border-neutral-800">
                      <p className="text-[10px] uppercase tracking-wider text-neutral-500 mb-0.5">MAP</p>
                      <p className={`font-mono font-medium ${patient.map_mean && patient.map_mean < 65 ? 'text-red-400' : 'text-neutral-300'}`}>
                        {patient.map_mean ? Math.round(patient.map_mean) : '-'}
                      </p>
                    </div>
                    <div className="text-center group border-l border-neutral-800">
                      <p className="text-[10px] uppercase tracking-wider text-neutral-500 mb-0.5">Lactate</p>
                      <p className={`font-mono font-medium ${patient.lactate && patient.lactate > 2.0 ? 'text-orange-400' : 'text-neutral-300'}`}>
                        {patient.lactate ? patient.lactate.toFixed(1) : '-'}
                      </p>
                    </div>
                    <div className="text-center group border-l border-neutral-800">
                      <p className="text-[10px] uppercase tracking-wider text-neutral-500 mb-0.5">SOFA</p>
                      <p className={`font-mono font-medium ${patient.sofa_proxy && patient.sofa_proxy >= 2 ? 'text-yellow-400' : 'text-neutral-300'}`}>
                        {patient.sofa_proxy ?? '-'}
                      </p>
                    </div>
                  </div>

                  {/* AI Reasoning Panel */}
                  <div className="flex-1 mt-auto space-y-3">
                    <div>
                      <p className="text-xs uppercase tracking-wider text-neutral-500 font-semibold mb-1">AI Primary Concern</p>
                      <p className="text-sm text-neutral-200 leading-relaxed font-medium">
                        {patient.primary_concern}
                      </p>
                    </div>
                    
                    {patient.risk_level === 'HIGH' && (
                      <div className="bg-blue-500/10 border border-blue-500/20 rounded-lg p-3">
                        <p className="text-xs uppercase tracking-wider text-blue-400/80 font-semibold mb-1">Recommended Action</p>
                        <p className="text-sm text-blue-100">{patient.recommended_action}</p>
                      </div>
                    )}
                  </div>
                  
                  {/* Confidence Footer */}
                  <div className="mt-4 pt-3 border-t border-neutral-800 flex justify-between items-center">
                    <span className="text-xs text-neutral-500">Claude 3.5 Sonnet</span>
                    <span className="text-xs text-neutral-400 font-medium">
                      Confidence: {(patient.confidence * 100).toFixed(0)}%
                    </span>
                  </div>

                </div>
              ))}
            </div>
          )}
        </div>

      </div>
    </div>
  );
}
