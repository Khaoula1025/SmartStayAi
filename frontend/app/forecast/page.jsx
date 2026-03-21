'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import { Download, Filter, Search, ChevronLeft, ChevronRight, Calendar as CalendarIcon, DollarSign, AlertCircle, BarChart as BarChartIcon, TrendingUp } from 'lucide-react';
import { 
  ComposedChart, Area, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell
} from 'recharts';
import { getPredictions } from '../lib/api';
import { formatOcc, formatRate, formatShortDate, formatDate, tierColor, qualityVariant } from '../lib/format';
import DashboardLayout from '../components/layout/DashboardLayout';
import Badge from '../components/ui/Badge';
import AlertBanner from '../components/ui/AlertBanner';
import LoadingSpinner from '../components/ui/LoadingSpinner';

export default function ForecastPage() {
  const [data, setData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Filters
  const today = new Date();
  const plus60 = new Date();
  plus60.setDate(today.getDate() + 60);
  
  const [dateFrom, setDateFrom] = useState(today.toISOString().split('T')[0]);
  const [dateTo, setDateTo] = useState(plus60.toISOString().split('T')[0]);
  const [rateTier, setRateTier] = useState('all');
  
  // Pagination
  const [currentPage, setCurrentPage] = useState(1);
  const rowsPerPage = 14;

  const loadData = useCallback(async (from, to) => {
    setIsLoading(true);
    setError(null);
    try {
      const result = await getPredictions(from, to);
      setData(result || []);
      setCurrentPage(1);
    } catch (err) {
      console.error('Failed to load forecast data:', err);
      setError('Could not load forecast data. Please try again later.');
    } finally {
      setIsLoading(false);
    }
  }, []);

  useEffect(() => {
    loadData(dateFrom, dateTo);
  }, [loadData, dateFrom, dateTo]);

  const handleApplyFilters = () => {
    loadData(dateFrom, dateTo);
  };

  const filteredData = useMemo(() => {
    if (rateTier === 'all') return data;
    return data.filter(item => item.rate_tier?.toLowerCase() === rateTier.toLowerCase());
  }, [data, rateTier]);

  // Summary Stats
  const summaryStats = useMemo(() => {
    if (!filteredData.length) return { avgOcc: 0, avgRate: 0, highPremiumDays: 0, eventDays: 0 };
    
    let totalOcc = 0;
    let totalRate = 0;
    let highPremiumDays = 0;
    let eventDays = 0;
    
    filteredData.forEach(d => {
      totalOcc += d.predicted_occ || 0;
      totalRate += d.recommended_rate || 0;
      if (['high', 'premium'].includes(d.rate_tier?.toLowerCase())) highPremiumDays++;
      if (d.flag === 'event') eventDays++;
    });
    
    return {
      avgOcc: totalOcc / filteredData.length,
      avgRate: totalRate / filteredData.length,
      highPremiumDays,
      eventDays
    };
  }, [filteredData]);

  // Chart Data Preparation
  const chartData = useMemo(() => {
    return filteredData.map(d => ({
      ...d,
      formattedDate: formatShortDate(d.date),
      occPercentage: Number((d.predicted_occ * 100).toFixed(1)),
      confidenceInterval: [
        Number((d.occ_low * 100).toFixed(1)), 
        Number((d.occ_high * 100).toFixed(1))
      ]
    }));
  }, [filteredData]);

  // Pagination
  const totalPages = Math.ceil(filteredData.length / rowsPerPage);
  const paginatedData = useMemo(() => {
    const start = (currentPage - 1) * rowsPerPage;
    return filteredData.slice(start, start + rowsPerPage);
  }, [filteredData, currentPage]);

  const handleExportCSV = () => {
    if (!filteredData.length) return;
    
    const headers = ['Date', 'Predicted Occ %', 'Occ Low %', 'Occ High %', 'Recommended Rate', 'Rate Tier', 'Pace Gap', 'Data Quality'];
    const csvContent = [
      headers.join(','),
      ...filteredData.map(d => [
        d.date,
        (d.predicted_occ * 100).toFixed(2),
        (d.occ_low * 100).toFixed(2),
        (d.occ_high * 100).toFixed(2),
        d.recommended_rate,
        d.rate_tier,
        d.pace_gap,
        d.data_quality
      ].join(','))
    ].join('\n');
    
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `smartstay_forecast_${dateFrom}_to_${dateTo}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  };

  const CustomOccTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-4 border border-border rounded-lg shadow-lg">
          <p className="font-bold text-navy mb-2">{formatDate(data.date)}</p>
          <div className="space-y-1 text-sm">
            <p className="flex justify-between gap-4">
              <span className="text-text-muted">Predicted Occ:</span>
              <span className="font-semibold text-text-dark">{formatOcc(data.predicted_occ)}</span>
            </p>
            <p className="flex justify-between gap-4">
              <span className="text-text-muted">Confidence:</span>
              <span className="font-semibold text-text-dark">{formatOcc(data.occ_low)} - {formatOcc(data.occ_high)}</span>
            </p>
            <div className="pt-2 mt-2 border-t border-border">
              <Badge 
                label={data.rate_tier} 
                style={{ backgroundColor: `${tierColor(data.rate_tier)}20`, color: tierColor(data.rate_tier) }}
                className="mt-1"
              />
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  const CustomRateTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-white p-4 border border-border rounded-lg shadow-lg">
          <p className="font-bold text-navy mb-2">{formatDate(data.date)}</p>
          <div className="space-y-1 text-sm">
            <p className="flex justify-between gap-6">
              <span className="text-text-muted">Recommended Rate:</span>
              <span className="font-semibold text-text-dark">{formatRate(data.recommended_rate)}</span>
            </p>
            <div className="pt-2 mt-2 border-t border-border flex items-center justify-between">
              <span className="text-text-muted">Tier:</span>
              <span className="font-semibold capitalize" style={{ color: tierColor(data.rate_tier) }}>{data.rate_tier}</span>
            </div>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <DashboardLayout>
      <div className="space-y-6">
        {/* Filter Bar */}
        <div className="bg-white p-4 rounded-xl shadow-sm border border-border flex flex-col md:flex-row gap-4 items-end md:items-center justify-between">
          <div className="flex flex-col md:flex-row gap-4 w-full md:w-auto">
            <div className="flex flex-col gap-1.5 flex-1 md:flex-none">
              <label className="text-xs font-semibold text-text-muted uppercase tracking-wider">Date From</label>
              <input 
                type="date" 
                value={dateFrom} 
                onChange={(e) => setDateFrom(e.target.value)}
                className="block w-full rounded-md border-0 py-2 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6"
              />
            </div>
            <div className="flex flex-col gap-1.5 flex-1 md:flex-none">
              <label className="text-xs font-semibold text-text-muted uppercase tracking-wider">Date To</label>
              <input 
                type="date" 
                value={dateTo} 
                onChange={(e) => setDateTo(e.target.value)}
                className="block w-full rounded-md border-0 py-2 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6"
              />
            </div>
            <div className="flex flex-col gap-1.5 flex-1 md:flex-none">
              <label className="text-xs font-semibold text-text-muted uppercase tracking-wider">Rate Tier</label>
              <select
                value={rateTier}
                onChange={(e) => setRateTier(e.target.value)}
                className="block w-full rounded-md border-0 py-2 pl-3 pr-8 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6 bg-white"
              >
                <option value="all">All Tiers</option>
                <option value="promotional">Promotional</option>
                <option value="value">Value</option>
                <option value="standard">Standard</option>
                <option value="high">High</option>
                <option value="premium">Premium</option>
              </select>
            </div>
          </div>
          <div className="flex gap-3 w-full md:w-auto">
            <button 
              onClick={handleApplyFilters}
              disabled={isLoading}
              className="flex-1 md:flex-none flex items-center justify-center rounded-md bg-navy px-4 py-2 text-sm font-semibold text-white shadow-sm hover:bg-navy-light focus-visible:outline focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-navy transition-colors disabled:opacity-70"
            >
              <Filter className="w-4 h-4 mr-2" />
              Apply Filters
            </button>
          </div>
        </div>

        {error && <AlertBanner message={error} type="error" />}

        {isLoading ? (
          <div className="bg-white rounded-xl shadow-sm border border-border p-12 min-h-[400px] flex items-center justify-center">
            <LoadingSpinner text="Forecasting future demand..." />
          </div>
        ) : filteredData.length === 0 ? (
          <div className="bg-white rounded-xl shadow-sm border border-border p-16 text-center">
            <Search className="w-12 h-12 text-border mx-auto mb-4" />
            <h3 className="text-lg font-medium text-navy">No forecast data found</h3>
            <p className="text-text-muted mt-1">Try adjusting your date range or filters.</p>
          </div>
        ) : (
          <>
            {/* Summary Stat Pills */}
            <div className="flex flex-wrap gap-4">
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-border flex items-center gap-3">
                <div className="p-2 rounded-full bg-navy/10 text-navy">
                  <CalendarIcon className="w-4 h-4" />
                </div>
                <div>
                  <p className="text-xs text-text-muted font-medium uppercase tracking-wider">Avg Predicted Occ</p>
                  <p className="text-lg font-bold text-text-dark">{formatOcc(summaryStats.avgOcc)}</p>
                </div>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-border flex items-center gap-3">
                <div className="p-2 rounded-full bg-gold/10 text-gold-dark">
                  <DollarSign className="w-4 h-4" />
                </div>
                <div>
                  <p className="text-xs text-text-muted font-medium uppercase tracking-wider">Avg Recommended Rate</p>
                  <p className="text-lg font-bold text-text-dark">{formatRate(summaryStats.avgRate)}</p>
                </div>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-border flex items-center gap-3">
                <div className="p-2 rounded-full bg-success/10 text-success">
                  <TrendingUp className="w-4 h-4" />
                </div>
                <div>
                  <p className="text-xs text-text-muted font-medium uppercase tracking-wider">High/Premium Days</p>
                  <p className="text-lg font-bold text-text-dark">{summaryStats.highPremiumDays}</p>
                </div>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-border flex items-center gap-3">
                <div className="p-2 rounded-full bg-warning/10 text-warning">
                  <AlertCircle className="w-4 h-4" />
                </div>
                <div>
                  <p className="text-xs text-text-muted font-medium uppercase tracking-wider">Event Flagged Days</p>
                  <p className="text-lg font-bold text-text-dark">{summaryStats.eventDays}</p>
                </div>
              </div>
            </div>

            {/* Charts Row */}
            <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
              {/* Occupancy Forecast Chart */}
              <div className="bg-white p-5 rounded-xl shadow-sm border border-border flex flex-col h-[400px]">
                <h3 className="text-lg font-bold text-navy mb-4 flex items-center gap-2">
                  <BarChartIcon className="w-5 h-5 text-gold" />
                  Demand Forecast & Confidence
                </h3>
                <div className="flex-1 w-full min-h-0">
                  <ResponsiveContainer width="100%" height="100%">
                    <ComposedChart data={chartData} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
                      <XAxis 
                        dataKey="formattedDate" 
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        dy={10}
                        minTickGap={30}
                      />
                      <YAxis 
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        domain={[0, 100]}
                        tickFormatter={(val) => `${val}%`}
                      />
                      <Tooltip content={<CustomOccTooltip />} />
                      <Area 
                        type="monotone" 
                        dataKey="confidenceInterval" 
                        stroke="none" 
                        fill="#E6FFFA" 
                        fillOpacity={0.7} 
                      />
                      <Line 
                        type="monotone" 
                        dataKey="occPercentage" 
                        stroke="var(--gold)" 
                        strokeWidth={2}
                        dot={false}
                        activeDot={{ r: 5, fill: "var(--navy)", stroke: "var(--gold)", strokeWidth: 2 }}
                      />
                    </ComposedChart>
                  </ResponsiveContainer>
                </div>
              </div>

              {/* Rate Recommendation Chart */}
              <div className="bg-white p-5 rounded-xl shadow-sm border border-border flex flex-col h-[400px]">
                <h3 className="text-lg font-bold text-navy mb-4 flex items-center gap-2">
                  <DollarSign className="w-5 h-5 text-gold" />
                  Recommended Rates by Tier
                </h3>
                <div className="flex-1 w-full min-h-0">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={chartData} margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
                      <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
                      <XAxis 
                        dataKey="formattedDate" 
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        dy={10}
                        minTickGap={30}
                      />
                      <YAxis 
                        axisLine={false}
                        tickLine={false}
                        tick={{ fill: '#718096', fontSize: 12 }}
                        tickFormatter={(val) => `£${val}`}
                      />
                      <Tooltip content={<CustomRateTooltip />} cursor={{ fill: '#F7F8FC' }} />
                      <Bar 
                        dataKey="recommended_rate" 
                        radius={[4, 4, 0, 0]}
                        maxBarSize={40}
                      >
                        {chartData.map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={tierColor(entry.rate_tier)} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </div>
            </div>

            {/* Data Table */}
            <div className="bg-white rounded-xl shadow-sm border border-border overflow-hidden flex flex-col">
              <div className="p-4 border-b border-border flex flex-col sm:flex-row justify-between items-center bg-surface/50">
                <h3 className="text-lg font-bold text-navy mb-2 sm:mb-0">Detailed Forecast Predictions</h3>
                <button 
                  onClick={handleExportCSV}
                  className="flex items-center text-sm font-medium text-navy hover:text-gold transition-colors"
                >
                  <Download className="w-4 h-4 mr-1.5" />
                  Export CSV
                </button>
              </div>
              
              <div className="overflow-x-auto">
                <table className="w-full text-sm text-left whitespace-nowrap">
                  <thead className="bg-white border-b border-border text-text-muted">
                    <tr>
                      <th className="px-5 py-3 font-semibold w-[120px]">Date</th>
                      <th className="px-5 py-3 font-semibold w-[80px]">Day</th>
                      <th className="px-5 py-3 font-semibold min-w-[150px]">Predicted Occ</th>
                      <th className="px-5 py-3 font-semibold text-right">Rooms</th>
                      <th className="px-5 py-3 font-semibold text-right">Rec. Rate</th>
                      <th className="px-5 py-3 font-semibold text-center">Tier</th>
                      <th className="px-5 py-3 font-semibold text-right">Pace Gap</th>
                      <th className="px-5 py-3 font-semibold text-center">Data Quality</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-border">
                    {paginatedData.map((row, idx) => {
                      const d = new Date(row.date);
                      const dayName = isNaN(d) ? '' : d.toLocaleDateString('en-GB', { weekday: 'short' });
                      const occPct = (row.predicted_occ || 0) * 100;
                      
                      return (
                        <tr key={idx} className="hover:bg-surface/30 transition-colors">
                          <td className="px-5 py-3 font-medium text-text-dark">
                            {formatShortDate(row.date)}
                          </td>
                          <td className="px-5 py-3 text-text-muted">
                            {dayName}
                          </td>
                          <td className="px-5 py-3">
                            <div className="flex items-center">
                              <span className="w-12 tabular-nums font-semibold">{occPct.toFixed(1)}%</span>
                              <div className="flex-1 ml-2 h-2 rounded-full bg-surface overflow-hidden">
                                <div 
                                  className="h-full rounded-full transition-all duration-500"
                                  style={{ 
                                    width: `${Math.min(100, occPct)}%`,
                                    backgroundColor: occPct >= 90 ? 'var(--gold)' : occPct >= 70 ? 'var(--navy)' : 'var(--text-muted)'
                                  }}
                                />
                              </div>
                            </div>
                          </td>
                          <td className="px-5 py-3 text-right tabular-nums text-text-muted">
                            {Math.round((row.predicted_occ || 0) * 52)}
                          </td>
                          <td className="px-5 py-3 text-right font-semibold text-text-dark tabular-nums">
                            {formatRate(row.recommended_rate)}
                          </td>
                          <td className="px-5 py-3 text-center">
                            <Badge 
                              label={row.rate_tier || 'Unknown'} 
                              style={{ 
                                backgroundColor: `${tierColor(row.rate_tier)}15`, 
                                color: tierColor(row.rate_tier),
                                borderColor: `${tierColor(row.rate_tier)}30`
                              }}
                              className="ring-1 ring-inset"
                            />
                          </td>
                          <td className="px-5 py-3 text-right">
                            <span className={`tabular-nums font-medium ${row.pace_gap > 0 ? 'text-success' : row.pace_gap < 0 ? 'text-danger' : 'text-text-muted'}`}>
                              {row.pace_gap > 0 ? '+' : ''}{row.pace_gap || 0}
                            </span>
                          </td>
                          <td className="px-5 py-3 text-center">
                            <Badge 
                              label={row.data_quality || 'Unknown'} 
                              variant={qualityVariant(row.data_quality)}
                            />
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="px-5 py-3 border-t border-border bg-surface flex items-center justify-between">
                  <div className="text-sm text-text-muted">
                    Showing <span className="font-medium">{(currentPage - 1) * rowsPerPage + 1}</span> to <span className="font-medium">{Math.min(currentPage * rowsPerPage, filteredData.length)}</span> of <span className="font-medium">{filteredData.length}</span> dates
                  </div>
                  <div className="flex items-center space-x-2">
                    <button
                      onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                      disabled={currentPage === 1}
                      className="p-1 rounded-md text-navy hover:bg-navy/10 disabled:opacity-30 disabled:hover:bg-transparent transition-colors"
                    >
                      <ChevronLeft className="w-5 h-5" />
                    </button>
                    <span className="text-sm font-medium px-2 text-text-dark">
                      Page {currentPage} of {totalPages}
                    </span>
                    <button
                      onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                      disabled={currentPage === totalPages}
                      className="p-1 rounded-md text-navy hover:bg-navy/10 disabled:opacity-30 disabled:hover:bg-transparent transition-colors"
                    >
                      <ChevronRight className="w-5 h-5" />
                    </button>
                  </div>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </DashboardLayout>
  );
}
