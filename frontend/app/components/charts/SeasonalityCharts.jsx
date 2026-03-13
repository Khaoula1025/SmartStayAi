'use client';

import { useState, useEffect } from 'react';
import { 
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, 
  Line, Area, ComposedChart, ReferenceLine, Legend, Cell
} from 'recharts';
import { TrendingUp, Info } from 'lucide-react';
import { getSeasonality } from '../../lib/api';
import LoadingSpinner from '../ui/LoadingSpinner';

export default function SeasonalityCharts({ data }) {
  if (!data) return null;

  const { yearly = [], weekly = [], trend = [], comparison_stats = {} } = data;

  const formatEffect = (val) => {
    const num = parseFloat(val);
    return num > 0 ? `+${num.toFixed(1)}pp` : `${num.toFixed(1)}pp`;
  };

  return (
    <div className="space-y-6 pb-12">
      {/* Card 1: Monthly Seasonality */}
      <div className="bg-white rounded-xl border border-border overflow-hidden shadow-sm p-6">
        <h3 className="text-xl font-bold text-navy">Yearly Demand Pattern</h3>
        <p className="text-sm text-text-muted mb-6">Which months drive occupancy at Hickstead</p>
        
        <div className="h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={yearly} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
              <XAxis 
                dataKey="month" 
                axisLine={false} 
                tickLine={false} 
                tick={{ fill: '#94A3B8', fontSize: 12 }} 
              />
              <YAxis 
                axisLine={false} 
                tickLine={false} 
                tick={{ fill: '#94A3B8', fontSize: 12 }}
                tickFormatter={(v) => `${v > 0 ? '+' : ''}${v}`}
              />
              <ReferenceLine y={0} stroke="#1C2B4A" strokeDasharray="3 3" />
              <Tooltip 
                cursor={{ fill: '#F1F5F9' }}
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    const val = payload[0].value;
                    return (
                      <div className="bg-white p-2 border border-border shadow-lg rounded text-xs">
                        <p className="font-bold text-navy">{label}</p>
                        <p className="text-text-dark">{label}: {formatEffect(val)} vs average</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Bar 
                dataKey="effect_pp" 
                radius={[4, 4, 0, 0]}
              >
                {yearly.map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={entry.effect_pp > 0 ? '#C9A84C' : '#94A3B8'} 
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
        
        <div className="mt-4 flex gap-4">
          <div className="bg-gold/10 text-gold-dark px-3 py-1.5 rounded-full text-xs font-bold">
            Peak: July +27.3pp
          </div>
          <div className="bg-danger/10 text-danger px-3 py-1.5 rounded-full text-xs font-bold">
            Trough: Jan -49.4pp
          </div>
        </div>
      </div>

      {/* Card 2: Weekly Demand Pattern */}
      <div className="bg-white rounded-xl border border-border overflow-hidden shadow-sm p-6">
        <h3 className="text-xl font-bold text-navy">Weekly Demand Pattern</h3>
        <p className="text-sm text-text-muted mb-6">Which days of the week perform best</p>
        
        <div className="h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <BarChart data={weekly} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
              <XAxis dataKey="day" axisLine={false} tickLine={false} tick={{ fill: '#94A3B8', fontSize: 12 }} />
              <YAxis axisLine={false} tickLine={false} tick={{ fill: '#94A3B8', fontSize: 12 }} />
              <ReferenceLine y={0} stroke="#1C2B4A" strokeDasharray="3 3" />
              <Tooltip 
                cursor={{ fill: '#F1F5F9' }}
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    const val = payload[0].value;
                    return (
                      <div className="bg-white p-2 border border-border shadow-lg rounded text-xs">
                        <p className="font-bold text-navy">{label}</p>
                        <p className="text-text-dark">{label}: {formatEffect(val)} vs average</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Bar dataKey="effect_pp">
                {weekly.map((entry, index) => (
                  <Cell 
                    key={`cell-${index}`} 
                    fill={entry.effect_pp > 0 ? '#C9A84C' : '#94A3B8'} 
                  />
                ))}
              </Bar>
            </BarChart>
          </ResponsiveContainer>
        </div>
        
        <div className="mt-6 bg-[#FEF9EC] border border-gold/30 p-4 rounded-lg flex items-start gap-3">
          <TrendingUp className="text-gold w-5 h-5 shrink-0 mt-0.5" />
          <p className="text-sm text-gold-darker leading-relaxed">
            Midweek demand (Tue +14.6pp, Wed +13.6pp) outperforms
            weekend at Hickstead — indicating a strong corporate segment.
          </p>
        </div>
      </div>

      {/* Card 3: Long-term Trend */}
      <div className="bg-white rounded-xl border border-border overflow-hidden shadow-sm p-6">
        <h3 className="text-xl font-bold text-navy">Occupancy Trend Since Opening</h3>
        <p className="text-sm text-text-muted mb-6">Hotel ramp-up from April 2024 through 2026 forecast</p>
        
        <div className="h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={trend} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
              <XAxis 
                dataKey="date" 
                axisLine={false} 
                tickLine={false} 
                tick={{ fill: '#94A3B8', fontSize: 10 }}
                tickFormatter={(v) => {
                  const d = new Date(v);
                  if (isNaN(d)) return v;
                  return d.toLocaleDateString('en-GB', { month: 'short', year: '2-digit' });
                }}
              />
              <YAxis domain={[0, 100]} axisLine={false} tickLine={false} tick={{ fill: '#94A3B8', fontSize: 12 }} />
              <Tooltip 
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    const d = new Date(label);
                    return (
                      <div className="bg-white p-2 border border-border shadow-lg rounded text-xs">
                        <p className="font-bold text-navy">{isNaN(d) ? label : d.toLocaleDateString('en-GB', { month: 'long', year: 'numeric' })}</p>
                        <p className="text-text-dark">Trend: {payload[0].value}%</p>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <ReferenceLine x="2026-01-01" stroke="#E53E3E" strokeDasharray="3 3" label={{ position: 'top', value: '2026 Forecast', fill: '#E53E3E', fontSize: 10 }} />
              <Line 
                type="monotone" 
                dataKey="trend" 
                stroke="#1C2B4A" 
                strokeWidth={2.5} 
                dot={false} 
              />
              <Line 
                type="monotone" 
                dataKey="forecast" 
                stroke="#C9A84C" 
                strokeWidth={2} 
                strokeDasharray="5 5" 
                dot={false} 
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
        
        <p className="mt-4 text-sm text-text-muted">
          Trend grew from 17.8% (hotel opening) to 70.6% (Dec 2025). 2026 forecast holds at ~67%.
        </p>
      </div>

      {/* Card 4: Model Agreement */}
      <div className="bg-white rounded-xl border border-border overflow-hidden shadow-sm p-6">
        <h3 className="text-xl font-bold text-navy">GBM + Random Forest vs Prophet — Forecast Agreement</h3>
        <p className="text-sm text-text-muted mb-6">When both models agree, confidence is higher</p>
        
        <div className="flex flex-wrap gap-3 mb-6">
          <div className="bg-navy text-white px-3 py-1 rounded-full text-xs font-semibold">
            50% dates agree
          </div>
          <div className="bg-slate-200 text-text-muted px-3 py-1 rounded-full text-xs font-semibold">
            Avg gap: 12.3pp
          </div>
          <div className="bg-gold/20 text-gold-dark px-3 py-1 rounded-full text-xs font-semibold uppercase tracking-wider">
            {comparison_stats.high_confidence_dates || 0} high-confidence dates
          </div>
        </div>

        <div className="bg-[#EEF1F7] p-4 rounded-lg flex items-start gap-3 mb-6">
          <Info className="text-navy w-5 h-5 shrink-0 mt-0.5" />
          <p className="text-sm text-navy/80 leading-relaxed">
            {comparison_stats.interpretation}
          </p>
        </div>

        <div className="h-[300px] w-full">
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={comparison_stats.forecast_data || []} margin={{ top: 10, right: 10, left: -20, bottom: 0 }}>
              <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#E2E8F0" />
              <XAxis 
                dataKey="date" 
                axisLine={false} 
                tickLine={false} 
                tick={{ fill: '#94A3B8', fontSize: 10 }}
                interval={30}
              />
              <YAxis domain={[0, 100]} axisLine={false} tickLine={false} tick={{ fill: '#94A3B8', fontSize: 12 }} />
              <Tooltip 
                content={({ active, payload, label }) => {
                  if (active && payload && payload.length) {
                    const data = payload[0].payload;
                    return (
                      <div className="bg-white p-3 border border-border shadow-xl rounded-lg text-xs min-w-[180px]">
                        <p className="font-bold text-navy border-b border-border pb-1 mb-2">{label}</p>
                        <div className="space-y-1.5">
                          <p className="flex justify-between">
                            <span className="text-text-muted">GBM+RF:</span>
                            <span className="font-bold text-navy">{data.predicted_occ}%</span>
                          </p>
                          <p className="flex justify-between">
                            <span className="text-text-muted">Prophet:</span>
                            <span className="font-bold text-gold">{data.prophet_occ}%</span>
                          </p>
                          <p className="flex justify-between border-t border-border pt-1 mt-1 font-bold">
                            <span className="text-text-muted">Gap:</span>
                            <span className={data.gap > 0 ? 'text-navy' : 'text-gold'}>{Math.abs(data.gap)}pp</span>
                          </p>
                          <p className="text-[10px] text-center pt-1 italic text-text-muted">
                            Models agree: {data.agree ? 'Yes' : 'No'}
                          </p>
                        </div>
                      </div>
                    );
                  }
                  return null;
                }}
              />
              <Legend verticalAlign="top" height={36} iconType="circle" />
              <Area 
                type="monotone" 
                dataKey="occ_high" 
                name="GBM CI"
                stroke="none" 
                fill="#1C2B4A" 
                fillOpacity={0.1} 
              />
              <Area 
                type="monotone" 
                dataKey="occ_low" 
                name="CI Lower"
                stroke="none" 
                fill="#FFFFFF" 
                fillOpacity={1} 
              />
              <Line 
                type="monotone" 
                dataKey="predicted_occ" 
                name="GBM+RF ensemble" 
                stroke="#1C2B4A" 
                strokeWidth={2} 
                dot={false} 
              />
              <Line 
                type="monotone" 
                dataKey="prophet_occ" 
                name="Prophet" 
                stroke="#C9A84C" 
                strokeWidth={1.5} 
                strokeDasharray="4 4" 
                dot={false} 
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
