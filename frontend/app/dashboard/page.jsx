'use client';

import { useEffect, useState, useMemo } from 'react';
import { Calendar, TrendingUp, AlertCircle, Activity, Box, Search, PlayCircle } from 'lucide-react';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts';
import { getDashboardSummary } from '../lib/api';
import { formatOcc, formatRate, formatShortDate, formatPercent } from '../lib/format';
import DashboardLayout from '../components/layout/DashboardLayout';
import KpiCard from '../components/ui/KpiCard';
import AlertBanner from '../components/ui/AlertBanner';
import Badge from '../components/ui/Badge';
import LoadingSpinner from '../components/ui/LoadingSpinner';

export default function DashboardPage() {
  const [data, setData] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function loadData() {
      try {
        const summary = await getDashboardSummary();
        setData(summary);
      } catch (err) {
        console.error('Failed to load dashboard:', err);
        setError('Could not load dashboard data. Please try again later.');
      } finally {
        setIsLoading(false);
      }
    }
    loadData();
  }, []);

  // Format data for the chart - we'll use highlights as a proxy for trend if dedicated trend isn't in backend schema
  // (Backend schema doesn't show forecast_trend, but highlights has 30 days usually)
  const chartData = useMemo(() => {
    if (!data?.highlights) return [];
    return [...data.highlights]
      .sort((a, b) => new Date(a.date) - new Date(b.date))
      .map(item => ({
        date: formatShortDate(item.date),
        occupancy: Number((item.predicted_occ * 100).toFixed(1))
      }));
  }, [data]);

  if (isLoading) return <DashboardLayout><LoadingSpinner text="Loading dashboard data..." /></DashboardLayout>;
  if (error) return <DashboardLayout><AlertBanner message={error} type="error" dismissible={false} /></DashboardLayout>;
  if (!data) return <DashboardLayout><div className="text-center py-10">No data available</div></DashboardLayout>;

  // Destructure based on backend schema
  const { occupancy, pace, model, highlights, alerts, bob_quality, hotel, as_of } = data;

  const getPaceTrend = (gap) => {
    if (gap > 0) return 'up';
    if (gap < 0) return 'down';
    return 'neutral';
  };

  const getPaceColor = (gap) => gap >= 0 ? 'success' : 'danger';

  const getFlagVariant = (type) => {
    switch(type) {
      case 'high_demand': return 'gold';
      case 'event': return 'info'; 
      case 'bank_holiday': return 'success'; 
      case 'low_quality': return 'danger'; 
      default: return 'muted';
    }
  };

  const formatFlagLabel = (type) => {
    return type.split('_').map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(' ');
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Alerts Section */}
        {alerts && alerts.length > 0 && (
          <div className="space-y-2 mb-6">
            {alerts.map((alertMessage, idx) => (
              <AlertBanner 
                key={idx} 
                message={alertMessage} 
                type="warning" 
              />
            ))}
          </div>
        )}

        {/* KPIs Row */}
        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
          <KpiCard
            title="Next 30 Days Occ."
            value={formatOcc(occupancy.next_30d_avg)}
            subtitle="Avg. predicted occupancy"
            icon={Calendar}
            color="navy"
          />
          <KpiCard
            title="Next 60 Days Occ."
            value={formatOcc(occupancy.next_60d_avg)}
            subtitle="Avg. predicted occupancy"
            icon={Calendar}
            color="navy"
          />
          <KpiCard
            title="Avg. Pace Gap"
            value={`${pace.avg_pace_gap > 0 ? '+' : ''}${pace.avg_pace_gap.toFixed(1)} rng`}
            subtitle={`${pace.dates_ahead} dates ahead, ${pace.dates_behind} behind`}
            icon={TrendingUp}
            trend={getPaceTrend(pace.avg_pace_gap)}
            color={getPaceColor(pace.avg_pace_gap)}
          />
          <KpiCard
            title="Model Accuracy"
            value={formatPercent(model.occ_accuracy_pct)}
            subtitle="Historical MAE performance"
            icon={Activity}
            color="gold"
          />
        </div>

        {/* Next 30 Days Forecast Chart & Highlights */}
        <div className="grid grid-cols-1 lg:grid-cols-5 gap-6">
          <div className="lg:col-span-3 bg-white rounded-xl shadow-sm border border-border p-6 flex flex-col">
            <h3 className="text-lg font-bold text-navy mb-4">Demand Trend (Next 30 Days)</h3>
            <div className="h-64 flex-1 w-full">
              {chartData.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <LineChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
                    <XAxis 
                      dataKey="date" 
                      axisLine={false}
                      tickLine={false}
                      tick={{ fill: '#718096', fontSize: 12 }}
                      dy={10}
                      minTickGap={20}
                    />
                    <YAxis 
                      axisLine={false}
                      tickLine={false}
                      tick={{ fill: '#718096', fontSize: 12 }}
                      domain={[0, 100]}
                      tickFormatter={(val) => `${val}%`}
                    />
                    <Tooltip 
                      contentStyle={{ borderRadius: '8px', border: '1px solid #E2E8F0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                      formatter={(value) => [`${value}%`, 'Occupancy']}
                      labelStyle={{ color: '#1A202C', fontWeight: 'bold', marginBottom: '4px' }}
                    />
                    <Line 
                      type="monotone" 
                      dataKey="occupancy" 
                      stroke="var(--gold)" 
                      strokeWidth={3}
                      dot={false}
                      activeDot={{ r: 6, fill: "var(--navy)", stroke: "var(--gold)", strokeWidth: 2 }}
                    />
                  </LineChart>
                </ResponsiveContainer>
              ) : (
                <div className="w-full h-full flex items-center justify-center bg-surface rounded-lg">
                  <p className="text-text-muted">No forecast data available</p>
                </div>
              )}
            </div>
          </div>

          <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-border flex flex-col overflow-hidden">
            <div className="p-5 border-b border-border break-words">
              <h3 className="text-lg font-bold text-navy">Upcoming Highlights</h3>
              <p className="text-xs text-text-muted mt-1">Flagged dates from {hotel}</p>
            </div>
            
            <div className="flex-1 overflow-auto">
              {highlights && highlights.length > 0 ? (
                <table className="w-full text-sm text-left">
                  <thead className="bg-surface text-text-muted sticky top-0">
                    <tr>
                      <th className="px-5 py-3 font-semibold">Date</th>
                      <th className="px-5 py-3 font-semibold">Occ%</th>
                      <th className="px-5 py-3 font-semibold">Rate</th>
                      <th className="px-5 py-3 font-semibold text-right">Flag</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {highlights.slice(0, 10).map((item, idx) => (
                      <tr key={idx} className="hover:bg-surface/50 transition-colors">
                        <td className="px-5 py-3 font-medium text-text-dark whitespace-nowrap">
                          {formatShortDate(item.date)}
                        </td>
                        <td className="px-5 py-3 tabular-nums font-semibold">
                          {formatOcc(item.predicted_occ)}
                        </td>
                        <td className="px-5 py-3 tabular-nums text-text-muted">
                          {formatRate(item.recommended_rate)}
                        </td>
                        <td className="px-5 py-3 text-right">
                          <Badge 
                            variant={getFlagVariant(item.flag)} 
                            label={formatFlagLabel(item.flag)} 
                          />
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              ) : (
                <div className="p-8 text-center flex flex-col items-center justify-center h-full">
                  <AlertCircle className="w-10 h-10 text-border mb-3" />
                  <p className="text-text-muted font-medium">No highlights detected</p>
                </div>
              )}
            </div>
          </div>
        </div>

        {/* Bottom Row */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
          {/* Model Info */}
          <div className="bg-white rounded-xl shadow-sm border border-border p-6 flex items-start">
            <div className="p-3 bg-navy/10 text-navy rounded-lg mr-4">
              <Box className="w-6 h-6" />
            </div>
            <div className="flex-1">
              <h3 className="text-base font-bold text-navy mb-1">Model Information</h3>
              <p className="text-sm text-text-muted mb-4">Latest deployed forecasting model</p>
              
              <div className="grid grid-cols-2 gap-y-3 gap-x-4 text-sm">
                <div>
                  <span className="text-text-muted block text-xs">Type</span>
                  <span className="font-semibold px-2 py-0.5 mt-1 inline-block bg-surface rounded text-text-dark">{model.model_type || 'XGBoost'}</span>
                </div>
                <div>
                  <span className="text-text-muted block text-xs">Last Trained</span>
                  <span className="font-semibold text-text-dark block mt-1">{model.last_trained || '-'}</span>
                </div>
                <div>
                  <span className="text-text-muted block text-xs">MAE (Operational)</span>
                  <span className="font-semibold text-text-dark block mt-1">{model.mae_operational ? `${model.mae_operational.toFixed(1)}%` : '-'}</span>
                </div>
                <div>
                  <span className="text-text-muted block text-xs">Accuracy %</span>
                  <span className="font-semibold text-text-dark block mt-1">{formatPercent(model.occ_accuracy_pct)}</span>
                </div>
              </div>
            </div>
          </div>

          {/* BOB Data Quality */}
          <div className="bg-white rounded-xl shadow-sm border border-border p-6 flex items-start">
            <div className="p-3 bg-gold/10 text-gold-dark rounded-lg mr-4">
              <Search className="w-6 h-6" />
            </div>
            <div className="flex-1">
              <div className="flex items-center justify-between mb-1">
                <h3 className="text-base font-bold text-navy">BOB Data Quality</h3>
                <Badge 
                  variant={bob_quality === 'high' ? 'success' : bob_quality === 'medium' ? 'warning' : 'danger'} 
                  label={`${bob_quality} Quality`} 
                />
              </div>
              <p className="text-sm text-text-muted mb-4">Quality check of PMS reservations data</p>
              
              <p className="text-sm text-text-dark font-medium leading-relaxed bg-surface p-3 rounded-lg border border-border">
                {bob_quality === 'high' ? 'Latest data extraction looks solid.' : 'Data quality issues detected. Forecast might be less accurate.'}
              </p>
              <p className="text-xs text-text-muted mt-4 flex items-center">
                <PlayCircle className="w-3.5 h-3.5 mr-1" />
                Last rescored: {model.last_rescore || 'Recent'} (As of: {as_of})
              </p>
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}
