'use client';

import { useState, useEffect, useMemo } from 'react';
import { Target, TrendingUp, AlertTriangle, AlertCircle, BarChart2, Hash, Star } from 'lucide-react';
import { 
  ComposedChart, Line, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Legend
} from 'recharts';
import { getAccuracy, getModelMetrics, getModelHistory, getSeasonality } from '../lib/api';
import { formatOcc, formatError, formatPercent, formatShortDate, formatDate } from '../lib/format';
import DashboardLayout from '../components/layout/DashboardLayout';
import EmptyState from '../components/ui/EmptyState';
import AlertBanner from '../components/ui/AlertBanner';
import LoadingSpinner from '../components/ui/LoadingSpinner';
import Badge from '../components/ui/Badge';
import SeasonalityCharts from '../components/charts/SeasonalityCharts';

export default function AnalyticsPage() {
  const [accuracyData, setAccuracyData] = useState(null);
  const [metricsData, setMetricsData] = useState(null);
  const [historyData, setHistoryData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  const [noActuals, setNoActuals] = useState(false);
  
  // Tab State
  const [activeTab, setActiveTab] = useState('accuracy');
  const [seasonalityData, setSeasonalityData] = useState(null);
  const [isFetchingSeasonality, setIsFetchingSeasonality] = useState(false);

  // Default to past 30 days for accuracy view
  const today = new Date();
  const minus30 = new Date();
  minus30.setDate(today.getDate() - 30);
  
  const [dateFrom, setDateFrom] = useState(minus30.toISOString().split('T')[0]);
  const [dateTo, setDateTo] = useState(today.toISOString().split('T')[0]);

  useEffect(() => {
    async function loadData() {
      setIsLoading(true);
      setError(null);
      setNoActuals(false);
      
      try {
        const [acc, met, hist] = await Promise.all([
          getAccuracy(dateFrom, dateTo).catch(e => {
            // Check specifically for empty/no actuals error
            if (e.message?.includes('404')) return { no_data: true };
            throw e;
          }),
          getModelMetrics().catch(() => null),
          getModelHistory().catch(() => [])
        ]);

        if (!acc || acc.no_data || !acc.rows || acc.rows.length === 0) {
          setNoActuals(true);
        } else {
          setAccuracyData(acc);
        }
        
        setMetricsData(met);
        setHistoryData(hist || []);
      } catch (err) {
        console.error('Failed to load analytics data:', err);
        setError('Could not load performance analytics. Please try again later.');
      } finally {
        setIsLoading(false);
      }
    }

    loadData();
  }, [dateFrom, dateTo]);

  const handleTabChange = async (tab) => {
    setActiveTab(tab);
    if (tab === 'seasonality' && !seasonalityData && !isFetchingSeasonality) {
      setIsFetchingSeasonality(true);
      try {
        const data = await getSeasonality();
        setSeasonalityData(data);
      } catch (err) {
        console.error('Failed to load seasonality:', err);
      } finally {
        setIsFetchingSeasonality(false);
      }
    }
  };

  // Transform daily accuracy for chart
  const chartData = useMemo(() => {
    if (!accuracyData?.rows) return [];
    
    return accuracyData.rows.map(d => ({
      date: formatShortDate(d.date),
      fullDate: d.date,
      actual: d.actual_occ != null ? Number((d.actual_occ * 100).toFixed(1)) : 0,
      predicted: d.predicted_occ != null ? Number((d.predicted_occ * 100).toFixed(1)) : 0,
      error: d.abs_error != null ? Number(d.abs_error.toFixed(1)) : 0,
      tier: d.rate_tier || 'N/A'
    }));
  }, [accuracyData]);

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-4 border border-border rounded-lg shadow-lg">
          <p className="font-bold text-navy mb-2">{formatDate(data.fullDate)}</p>
          <div className="space-y-1.5 text-sm w-48">
            <p className="flex justify-between items-center text-text-muted font-medium">
              <span>Actual:</span>
              <span className="text-navy">{formatOcc(data.actual / 100)}</span>
            </p>
            <p className="flex justify-between items-center text-text-muted font-medium">
              <span>Predicted:</span>
              <span className="text-gold">{formatOcc(data.predicted / 100)}</span>
            </p>
            <div className="pt-2 mt-2 border-t border-border flex justify-between items-center">
              <span className="text-danger flex items-center font-medium">Abs. Error</span>
              <span className="text-danger font-bold">{data.error}pp</span>
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  const bestRun = useMemo(() => {
    if (!historyData || historyData.length === 0) return null;
    return [...historyData]
      .filter(r => r.occ_accuracy_pct != null)
      .sort((a, b) => b.occ_accuracy_pct - a.occ_accuracy_pct)[0];
  }, [historyData]);

  const getRunIdDisplay = (id) => {
    if (!id) return '-';
    return id.substring(0, 8);
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex flex-col sm:flex-row justify-between items-end sm:items-center gap-4">
          <div>
            <h2 className="text-2xl font-bold tracking-tight text-navy">Performance Analytics</h2>
            <p className="text-sm text-text-muted mt-1">Model accuracy and historical tracking</p>
          </div>
          
          <div className="flex bg-white rounded-md shadow-sm border border-border overflow-hidden">
            <input 
              type="date" 
              value={dateFrom} 
              onChange={(e) => setDateFrom(e.target.value)}
              className="border-0 focus:ring-0 py-2 sm:text-sm"
            />
            <span className="py-2 px-3 bg-surface text-text-muted text-sm border-x border-border">to</span>
            <input 
              type="date" 
              value={dateTo} 
              onChange={(e) => setDateTo(e.target.value)}
              className="border-0 focus:ring-0 py-2 sm:text-sm"
            />
          </div>
        </div>

        {/* Tabs */}
        <div className="flex border-b border-border">
          <button
            onClick={() => handleTabChange('accuracy')}
            className={`px-6 py-3 text-sm font-bold transition-all border-b-2 ${
              activeTab === 'accuracy' 
                ? 'border-navy text-gold' 
                : 'border-transparent text-text-muted hover:text-text-dark'
            }`}
          >
            Accuracy
          </button>
          <button
            onClick={() => handleTabChange('seasonality')}
            className={`px-6 py-3 text-sm font-bold transition-all border-b-2 ${
              activeTab === 'seasonality' 
                ? 'border-navy text-gold' 
                : 'border-transparent text-text-muted hover:text-text-dark'
            }`}
          >
            Seasonality
          </button>
        </div>

        {error && <AlertBanner message={error} type="error" />}

        {activeTab === 'accuracy' ? (
          isLoading ? (
            <div className="bg-white rounded-xl shadow-sm border border-border p-12 min-h-[400px] flex items-center justify-center">
              <LoadingSpinner text="Computing performance metrics..." />
            </div>
          ) : historyData.length === 0 ? (
            <EmptyState
              icon={BarChart2}
              title="No Model History"
              description="No models have been trained or tracked in the system yet."
            />
          ) : (
            <>
            {/* Summary Stat Pills */}
            {!noActuals && (
              <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
                <div className="bg-white p-5 rounded-xl shadow-sm border border-border">
                  <p className="text-sm font-medium text-text-muted uppercase tracking-wider flex items-center mb-2">
                    <Target className="w-4 h-4 mr-2" /> Mean Abs Error (MAE)
                  </p>
                  <div className="flex items-baseline gap-2">
                    <p className="text-4xl font-bold tracking-tight tabular-nums text-text-dark">
                      {accuracyData?.mean_abs_error_pp != null ? accuracyData.mean_abs_error_pp.toFixed(1) : '-'}
                    </p>
                    <span className="text-lg font-medium text-text-muted">pp</span>
                  </div>
                </div>
                
                <div className="bg-white p-5 rounded-xl shadow-sm border border-border">
                  <p className="text-sm font-medium text-text-muted uppercase tracking-wider flex items-center mb-2">
                    <TrendingUp className="w-4 h-4 mr-2" /> % Within CI
                  </p>
                  <div className="flex items-baseline gap-2">
                    <p className="text-4xl font-bold tracking-tight text-navy tabular-nums">
                      {formatPercent(accuracyData?.within_ci_pct)}
                    </p>
                  </div>
                </div>
                
                <div className="bg-white p-5 rounded-xl shadow-sm border border-border">
                  <p className="text-sm font-medium text-text-muted uppercase tracking-wider flex items-center mb-2">
                    <AlertTriangle className="w-4 h-4 text-warning mr-2" /> Worst Tier
                  </p>
                  <p className="text-2xl font-bold capitalize text-text-dark mb-1">
                    {accuracyData?.worst_tier || '-'}
                  </p>
                  <p className="text-sm text-text-muted">Highest error rate</p>
                </div>

                <div className="bg-white p-5 rounded-xl shadow-sm border border-border">
                  <p className="text-sm font-medium text-text-muted uppercase tracking-wider flex items-center mb-2">
                    <Badge label="Best Tier" variant="success" className="mr-2" />
                  </p>
                  <p className="text-2xl font-bold capitalize text-text-dark mb-1">
                    {accuracyData?.best_tier || '-'}
                  </p>
                  <p className="text-sm text-text-muted">Most consistent accuracy</p>
                </div>
              </div>
            )}

            {/* Model History Summary Card */}
            {bestRun && (
              <div className="mt-6 mb-4 p-5 bg-[#FEF9EC] border border-gold rounded-xl flex items-center justify-between text-navy shadow-sm">
                <div className="flex items-center gap-4">
                  <div className="p-2.5 bg-gold/20 rounded-lg">
                    <Star className="w-5 h-5 text-gold-dark fill-current" />
                  </div>
                  <div>
                    <p className="text-sm font-bold">Champion Model Performance</p>
                    <p className="text-xs text-text-muted">Best historical run from {new Date(bestRun.trained_at).toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' })}</p>
                  </div>
                </div>
                <div className="font-bold text-sm md:text-base">
                  Best Model: <span className="text-gold-darker">{formatPercent(bestRun.occ_accuracy_pct)} accuracy</span> 
                  <span className="mx-2 text-border">|</span> MAE: {bestRun.mae_operational != null ? Number(bestRun.mae_operational).toFixed(4) : '-'} 
                  <span className="mx-2 text-border">|</span> {bestRun.n_prediction_rows || 0} training rows
                </div>
              </div>
            )}

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              {/* Accuracy Chart */}
              <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-border p-5 h-[400px] flex flex-col">
                <h3 className="text-lg font-bold text-navy mb-4 flex items-center">
                  <BarChart2 className="w-5 h-5 mr-2 text-navy" />
                  Actual vs Predicted & Error Margin
                </h3>
                
                <div className="flex-1 w-full min-h-0 relative">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={chartData} margin={{ top: 5, right: 30, left: 10, bottom: 5 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
                      <XAxis 
                        dataKey="date" 
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        dy={10}
                      />
                      <YAxis 
                        yAxisId="left"
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        domain={[0, 100]}
                        tickFormatter={(val) => `${val}%`}
                      />
                      <YAxis 
                        yAxisId="right"
                        orientation="right"
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#E53E3E', fontSize: 12 }}
                        domain={[0, 'dataMax + 5']}
                        tickFormatter={(val) => `${val}pp`}
                      />
                      <Tooltip content={CustomTooltip} />
                      <Legend 
                        verticalAlign="top" 
                        height={36} 
                        iconType="circle"
                        wrapperStyle={{ fontSize: '13px', paddingTop: '0' }}
                      />
                      
                      <Bar 
                        yAxisId="right" 
                        dataKey="error" 
                        name="Abs. Error (pp)" 
                        fill="#FC8181" 
                        radius={[2, 2, 0, 0]}
                        opacity={0.5}
                        barSize={20}
                      />
                      <Line 
                        yAxisId="left"
                        type="monotone" 
                        dataKey="actual" 
                        name="Actual Occ"
                        stroke="var(--navy)" 
                        strokeWidth={2}
                        dot={{ r: 3, fill: "var(--navy)", strokeWidth: 0 }}
                      />
                      <Line 
                        yAxisId="left"
                        type="monotone" 
                        dataKey="predicted" 
                        name="Predicted Occ"
                        stroke="var(--gold)" 
                        strokeWidth={2}
                        strokeDasharray="5 5"
                        dot={false}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Tier Breakdown Table */}
              <div className="bg-white rounded-xl shadow-sm border border-border flex flex-col h-[400px]">
                <div className="p-5 border-b border-border bg-surface/50 shrink-0">
                  <h3 className="text-lg font-bold text-navy">Error Breakdown by Tier</h3>
                </div>
                
                <div className="flex-1 overflow-auto">
                  <table className="w-full text-sm text-left">
                    <thead className="bg-white text-text-muted sticky top-0 shadow-sm">
                      <tr>
                        <th className="px-5 py-3 font-semibold">Rate Tier</th>
                        <th className="px-5 py-3 font-semibold text-right">Dates</th>
                        <th className="px-5 py-3 font-semibold text-right">Mean Error</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {accuracyData?.by_tier && Object.entries(accuracyData.by_tier).map(([tier, stats]) => (
                        <tr key={tier} className="hover:bg-surface/50 transition-colors">
                          <td className="px-5 py-3 font-medium capitalize text-text-dark">
                            {tier}
                          </td>
                          <td className="px-5 py-3 text-right tabular-nums text-text-muted">
                            {stats.n_dates}
                          </td>
                          <td className="px-5 py-3 text-right tabular-nums font-semibold">
                            {formatError(stats.mean_abs_error_pp)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            {/* Model History Table */}
            <div className="bg-white rounded-xl shadow-sm border border-border flex flex-col mt-6">
              <div className="p-5 border-b border-border bg-surface/50 flex items-center justify-between">
                <div>
                  <h3 className="text-lg font-bold text-navy flex items-center">
                    <Hash className="w-5 h-5 mr-2 text-gold" />
                    Model Training History
                  </h3>
                  <p className="text-sm text-text-muted mt-1">Audit log of prediction models</p>
                </div>
              </div>
              
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left">
                  <thead className="bg-white border-b border-border text-text-muted">
                    <tr>
                      <th className="px-5 py-3 font-semibold">Run ID</th>
                      <th className="px-5 py-3 font-semibold">Trained Date</th>
                      <th className="px-5 py-3 font-semibold">Model Type</th>
                      <th className="px-5 py-3 font-semibold text-right">MAE</th>
                      <th className="px-5 py-3 font-semibold text-right">Accuracy %</th>
                      <th className="px-5 py-3 font-semibold text-center">Promoted</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {historyData && historyData.length > 0 ? historyData.map((hist, idx) => (
                      <tr 
                        key={idx} 
                        className={`transition-colors border-l-4 ${hist.occ_accuracy_pct != null ? 'border-l-gold bg-gold/5' : 'border-l-transparent'} ${hist.promoted ? 'bg-gold/5 hover:bg-gold/10' : 'hover:bg-surface/50'}`}
                      >
                        <td className="px-5 py-3 font-mono text-xs text-text-dark">
                          {getRunIdDisplay(hist.run_id)}
                        </td>
                        <td className="px-5 py-3 text-text-muted">
                          {hist.trained_at ? new Date(hist.trained_at).toLocaleString('en-GB', { dateStyle: 'medium', timeStyle: 'short' }) : '-'}
                        </td>
                        <td className="px-5 py-3">
                          <Badge label={hist.model_type || 'Daily Rescore'} variant={hist.model_type === 'retrain' ? 'success' : 'muted'} className="text-[10px] uppercase font-bold" />
                        </td>
                        <td className="px-5 py-3 text-right tabular-nums">
                          {hist.mae_operational != null ? formatError(hist.mae_operational) : '-'}
                        </td>
                        <td className="px-5 py-3 text-right tabular-nums font-semibold">
                          {hist.occ_accuracy_pct != null ? formatPercent(hist.occ_accuracy_pct) : '-'}
                        </td>
                        <td className="px-5 py-3 text-center">
                          {hist.promoted ? (
                            <div className="inline-flex justify-center p-1.5 bg-gold/20 rounded-full text-gold-dark">
                              <Star className="w-4 h-4 fill-current" />
                            </div>
                          ) : (
                            <span className="text-text-muted opacity-50">-</span>
                          )}
                        </td>
                      </tr>
                    )) : (
                      <tr>
                        <td colSpan="6" className="px-5 py-8 text-center text-text-muted">
                          No model history available.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
            </>
          )
        ) : (
          isFetchingSeasonality ? (
            <div className="bg-white rounded-xl shadow-sm border border-border p-12 min-h-[400px] flex items-center justify-center">
              <LoadingSpinner text="Analyzing demand cycles..." />
            </div>
          ) : (
            <SeasonalityCharts data={seasonalityData} />
          )
        )}
      </div>
    </DashboardLayout>
  );
}
