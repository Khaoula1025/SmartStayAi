'use client';

import { useState, useEffect } from 'react';
import { Play, CheckCircle2, XCircle, Clock, Database, AlertCircle, RefreshCw, ArrowRight, Settings, FileSpreadsheet, Server, LineChart, ShieldAlert } from 'lucide-react';
import { triggerPipeline } from '../lib/api';
import { useAuth } from '../context/AuthContext';
import DashboardLayout from '../components/layout/DashboardLayout';
import AlertBanner from '../components/ui/AlertBanner';
import LoadingSpinner from '../components/ui/LoadingSpinner';
import Badge from '../components/ui/Badge';
import Modal from '../components/ui/Modal';

// Components for diagram
const DiagramNode = ({ icon: Icon, title, desc }) => (
  <div className="flex flex-col items-center bg-white border-2 border-navy rounded-lg p-3 w-32 shadow-sm relative z-10 transition-transform hover:scale-105">
    <div className="p-2 bg-navy/10 text-navy rounded-full mb-2">
      <Icon className="w-5 h-5" />
    </div>
    <span className="text-xs font-bold text-navy text-center leading-tight">{title}</span>
    <span className="text-[10px] text-text-muted text-center mt-1">{desc}</span>
  </div>
);

const DiagramArrow = () => (
  <div className="flex-1 flex items-center justify-center -mx-2 z-0 relative">
    <div className="h-1 w-full bg-gradient-to-r from-gold/50 to-gold relative px-2">
      <div className="absolute right-0 top-1/2 -translate-y-1/2 translate-x-1.5 w-0 h-0 border-t-4 border-t-transparent border-b-4 border-b-transparent border-l-6 border-l-gold"></div>
    </div>
  </div>
);

export default function PipelinePage() {
  const { user } = useAuth();
  const [pipelineData, setPipelineData] = useState(null);
  const [connected, setConnected] = useState(false);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [isPolling, setIsPolling] = useState(false);
  
  // Trigger state
  const [isTriggerModalOpen, setIsTriggerModalOpen] = useState(false);
  const [triggerSteps, setTriggerSteps] = useState('');
  const [isTriggering, setIsTriggering] = useState(false);

  async function fetchPipelineStatus() {
    // Step 1: try analytics history
    try {
      const r1 = await fetch(
        'http://localhost:8000/api/v1/analytics/model/history?limit=10',
        { credentials: 'include' }
      )
      if (r1.ok) {
        const runs = await r1.json()
        if (runs && runs.length > 0) {
          const latest = runs[0]
          setPipelineData({
            status: 'operational',
            last_run: latest.trained_at,
            model_type: latest.model_type,
            n_runs: runs.length,
            n_prediction_rows: latest.n_prediction_rows || 0,
          })
          setConnected(true)
          setIsLoading(false)
          return
        }
      }
    } catch(e) {}

    // Step 2: try health endpoint
    try {
      const r2 = await fetch(
        'http://localhost:8000/health',
        { credentials: 'include' }
      )
      if (r2.ok) {
        setPipelineData({ status: 'healthy', n_runs: 0 })
        setConnected(true)
        setIsLoading(false)
        return
      }
    } catch(e) {}

    // All failed
    setConnected(false)
    setIsLoading(false)
  }

  useEffect(() => {
    fetchPipelineStatus();
    
    let interval;
    if (isPolling || pipelineData?.status === 'running') {
      interval = setInterval(fetchPipelineStatus, 5000);
    }
    
    return () => clearInterval(interval);
  }, [isPolling, pipelineData?.status]);

  const handleTrigger = async () => {
    setIsTriggering(true);
    setError(null);
    try {
      await triggerPipeline(triggerSteps.trim() || '');
      setIsTriggerModalOpen(false);
      setIsPolling(true);
      // Immediately set optimistic running state
      setPipelineData(prev => ({
        ...prev,
        status: 'running',
        started_at: new Date().toISOString(),
      }));
    } catch (err) {
      console.error('Failed to trigger pipeline:', err);
      alert('Failed to start the pipeline run. Please try again.');
    } finally {
      setIsTriggering(false);
    }
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString('en-GB', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
      hour: '2-digit',
      minute: '2-digit'
    });
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div>
          <h2 className="text-2xl font-bold tracking-tight text-navy">Pipeline Diagnostics</h2>
          <p className="text-sm text-text-muted mt-1">ETL and Model Training Status</p>
        </div>

        {error && <AlertBanner message={error} type="error" />}

        {isLoading ? (
          <div className="bg-white rounded-xl shadow-sm border border-border p-12 min-h-[300px] flex items-center justify-center">
            <LoadingSpinner text="Connecting to pipeline service..." />
          </div>
        ) : (
          <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
            
            {/* Last Run Status Card */}
            {!connected ? (
              <div className="lg:col-span-2 bg-slate-50 rounded-xl border-2 border-dashed border-slate-200 p-12 flex flex-col items-center justify-center text-center">
                <Server className="w-12 h-12 text-slate-300 mb-4" />
                <h3 className="text-lg font-bold text-slate-600">Pipeline Status Unavailable</h3>
                <p className="text-sm text-slate-500 max-w-xs">
                  Start the FastAPI backend to see live pipeline status
                </p>
              </div>
            ) : (
              <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-border flex flex-col overflow-hidden relative">
                {pipelineData?.status === 'running' && (
                  <div className="absolute top-0 left-0 w-full h-1 bg-surface z-10 overflow-hidden">
                    <div className="h-full bg-gold w-1/3 animate-[shimmer_2s_infinite]"></div>
                  </div>
                )}
                
                <div className="p-6 border-b border-border bg-[#F0FDF4] flex justify-between items-start">
                  <div>
                    <h3 className="text-lg font-bold text-navy flex items-center mb-2">
                       <span className="w-2.5 h-2.5 bg-success rounded-full mr-2 shadow-[0_0_8px_rgba(34,197,94,0.6)]"></span>
                       Pipeline Operational
                       {pipelineData?.status === 'running' && (
                         <RefreshCw className="w-4 h-4 ml-2 text-gold animate-spin" />
                       )}
                    </h3>
                    <div className="flex items-center text-sm text-text-muted gap-4">
                      <span className="flex items-center">
                        <Clock className="w-3.5 h-3.5 mr-1" />
                        Last run: {formatDate(pipelineData?.last_run || pipelineData?.finished_at || pipelineData?.started_at)}
                      </span>
                      <span className="flex items-center font-medium">
                        Model: <Badge label={pipelineData?.model_type || 'ensemble'} variant="muted" className="ml-1.5 uppercase text-[10px]" />
                      </span>
                    </div>
                  </div>
                  
                  <div className="flex flex-col items-end">
                    <Badge variant="success" label="Healthy" className="px-3 py-1" />
                    <span className="text-xs text-text-muted mt-2 tracking-wide font-medium flex items-center">
                      <Database className="w-3.5 h-3.5 mr-1" />
                      {pipelineData?.n_runs || 0} runs completed
                    </span>
                  </div>
                </div>
                
                <div className="p-8 flex-1 flex flex-col items-center justify-center text-center">
                   <div className="grid grid-cols-1 sm:grid-cols-2 gap-4 w-full max-w-lg">
                     <div className="bg-surface p-4 rounded-xl border border-border">
                        <p className="text-[10px] text-text-muted uppercase font-bold tracking-wider mb-1">Status</p>
                        <p className="text-lg font-bold text-navy capitalize">{pipelineData?.status}</p>
                     </div>
                     <div className="bg-surface p-4 rounded-xl border border-border">
                        <p className="text-[10px] text-text-muted uppercase font-bold tracking-wider mb-1">Scored Dates</p>
                        <p className="text-lg font-bold text-navy">{pipelineData?.n_prediction_rows || 0}</p>
                     </div>
                   </div>
                   
                   <div className="mt-8 flex items-center gap-2 text-success text-sm font-medium bg-success/5 px-4 py-2 rounded-full border border-success/10">
                     <CheckCircle2 className="w-4 h-4" />
                     <span>Predictions: {pipelineData?.n_prediction_rows || 0} dates scored</span>
                   </div>
                </div>
              </div>
            )}

            {/* Trigger Section (Admin Only) */}
            {user?.role === 'admin' ? (
              <div className="bg-white rounded-xl shadow-sm border border-border overflow-hidden h-fit">
                <div className="p-5 border-b border-border bg-navy text-white">
                  <h3 className="text-lg font-bold flex items-center">
                    <Settings className="w-5 h-5 mr-2 text-gold" />
                    Pipeline Controls
                  </h3>
                </div>
                
                <div className="p-5 space-y-4">
                  <p className="text-sm text-text-muted leading-relaxed">
                    Manually trigger the SmartStay ETL pipeline. Be aware that running the full pipeline can take 10-20 minutes depending on data volume.
                  </p>
                  
                  <div>
                    <label className="block text-sm font-bold text-text-dark mb-1">
                      Target Steps (Optional)
                    </label>
                    <input
                      type="text"
                      placeholder="e.g. 6,7,10"
                      value={triggerSteps}
                      onChange={(e) => setTriggerSteps(e.target.value)}
                      disabled={!connected || pipelineData?.status === 'running'}
                      className="block w-full rounded-md border-0 py-2.5 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6 disabled:bg-surface disabled:opacity-75"
                    />
                    <p className="text-xs text-text-muted mt-1 italic">Leave blank for full run.</p>
                  </div>
                  
                  <button
                    onClick={() => setIsTriggerModalOpen(true)}
                    disabled={!connected || pipelineData?.status === 'running'}
                    className="w-full flex justify-center items-center rounded-md bg-navy px-4 py-3 text-sm font-bold text-white shadow hover:bg-gold hover:text-navy focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 transition-all disabled:opacity-50 disabled:cursor-not-allowed group relative overflow-hidden"
                  >
                    {/* Shine effect */}
                    <span className="absolute inset-0 w-full h-full bg-gradient-to-r from-transparent via-white/10 to-transparent -translate-x-full group-hover:animate-[shimmer_1.5s_infinite]"></span>
                    
                    <Play className="w-4 h-4 mr-2 fill-current" />
                    <span>Trigger Pipeline</span>
                  </button>
                </div>
              </div>
            ) : (
              <div className="bg-surface rounded-xl shadow-inner border border-border p-6 text-center h-fit flex flex-col items-center">
                <ShieldAlert className="w-10 h-10 text-border mb-3" />
                <h3 className="text-sm font-bold text-text-muted">Admin Access Required</h3>
                <p className="text-xs text-text-muted mt-1">You do not have permission to manually trigger the pipeline.</p>
              </div>
            )}
          </div>
        )}

        {/* Architecture Diagram */}
        <div className="mt-8 bg-white rounded-xl shadow-sm border border-border p-8 overflow-x-auto relative hidden md:block">
          <h3 className="text-lg font-bold text-navy mb-8 text-center uppercase tracking-widest border-b border-border pb-4">
            SmartStay Architecture Flow
          </h3>
          
          <div className="flex items-center justify-between min-w-[700px] max-w-4xl mx-auto px-4 pb-4">
            <DiagramNode icon={FileSpreadsheet} title="Raw Data" desc="PMS Extracts" />
            <DiagramArrow />
            <DiagramNode icon={Database} title="Clean" desc="Steps 01-05" />
            <DiagramArrow />
            <DiagramNode icon={Server} title="Build Matrix" desc="Feature Eng" />
            <DiagramArrow />
            <DiagramNode icon={LineChart} title="Model" desc="Training Phase" />
            <DiagramArrow />
            <DiagramNode icon={RefreshCw} title="Rescore" desc="Daily Predictions" />
            <DiagramArrow />
            <DiagramNode icon={ArrowRight} title="API" desc="Dashboard Ready" />
          </div>
        </div>

        {/* Trigger Confirmation Modal */}
        <Modal
          isOpen={isTriggerModalOpen}
          onClose={() => !isTriggering && setIsTriggerModalOpen(false)}
          title="Confirm Pipeline Run"
          footer={
            <>
              <button
                onClick={() => setIsTriggerModalOpen(false)}
                disabled={isTriggering}
                className="px-4 py-2 bg-transparent text-text-muted hover:text-text-dark hover:bg-gray-100 font-medium rounded-md transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleTrigger}
                disabled={isTriggering}
                className="px-4 py-2 bg-navy text-white hover:bg-gold hover:text-navy font-bold rounded-md transition-colors flex items-center shadow-md disabled:opacity-50"
              >
                {isTriggering ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Starting...
                  </>
                ) : (
                  <>
                    <Play className="w-4 h-4 mr-2 fill-current" />
                    Yes, Run Pipeline
                  </>
                )}
              </button>
            </>
          }
        >
          <div className="bg-warning/10 text-warning-dark p-4 rounded-lg flex items-start border border-warning/20">
            <AlertCircle className="w-5 h-5 flex-shrink-0 mr-3 mt-0.5" />
            <div>
              <p className="font-bold mb-1">Are you sure you want to trigger the pipeline?</p>
              <p className="text-sm">
                This will run the full ETL pipeline process in the background. Operations may take 10-20 minutes depending on cluster load.
              </p>
              {triggerSteps && (
                <p className="text-sm mt-3 pt-3 border-t border-warning/20 font-medium">
                  Running specific steps: <span className="font-mono bg-warning/20 px-1.5 py-0.5 rounded text-xs">{triggerSteps}</span>
                </p>
              )}
            </div>
          </div>
        </Modal>
      </div>
    </DashboardLayout>
  );
}
