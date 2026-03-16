'use client';

import { useState, useEffect, useMemo } from 'react';
import { 
  TrendingUp, 
  ThumbsUp, 
  ThumbsDown, 
  Star, 
  Info, 
  MessageSquare,
  Clock,
  ArrowRight,
  Activity
} from 'lucide-react';
import { 
  ComposedChart, 
  Line, 
  Bar, 
  XAxis, 
  YAxis, 
  CartesianGrid, 
  Tooltip, 
  ResponsiveContainer, 
  Legend, 
  ReferenceLine,
  LineChart
} from 'recharts';
import { getSentimentSummary } from '../lib/api';
import DashboardLayout from '../components/layout/DashboardLayout';
import LoadingSpinner from '../components/ui/LoadingSpinner';
import Badge from '../components/ui/Badge';

export default function SentimentPage() {
  const [summary, setSummary] = useState(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);

  useEffect(() => {
    async function fetchData() {
      try {
        const data = await getSentimentSummary();
        setSummary(data);
      } catch (err) {
        console.error('Failed to fetch sentiment summary:', err);
        setError('Failed to load sentiment data. Please ensure the backend is running.');
      } finally {
        setIsLoading(false);
      }
    }
    fetchData();
  }, []);

  const formatDate = (val) => {
    if (!val) return '';
    const [y, m] = val.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${months[parseInt(m)-1]} ${y.slice(2)}`;
  };

  const formatFullDate = (date) => {
    if (!date) return '';
    const [y, m] = date.split('-');
    const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
    return `${months[parseInt(m)-1]} ${y}`;
  };

  const renderStars = (rating) => {
    const stars = [];
    const fullStars = Math.floor(rating);
    const hasHalfStar = rating % 1 >= 0.5;

    for (let i = 1; i <= 5; i++) {
      if (i <= fullStars) {
        stars.push(<span key={i} className="text-gold">★</span>);
      } else if (i === fullStars + 1 && hasHalfStar) {
        stars.push(<span key={i} className="text-gold">★</span>); // Simplified for now, real half-star would need SVG
      } else {
        stars.push(<span key={i} className="text-gray-300">★</span>);
      }
    }
    return stars;
  };

  // Monthly data filtered for trend chart
  const trendData = useMemo(() => {
    if (!summary?.monthly) return [];
    return summary.monthly.filter(m => m.n_reviews >= 2);
  }, [summary]);

  // filtered for correlation view (2024 onwards)
  const correlationData = useMemo(() => {
    if (!summary?.monthly) return [];
    return summary.monthly.filter(m => m.year_month >= '2024-01');
  }, [summary]);

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="min-h-[400px] flex items-center justify-center">
          <LoadingSpinner text="Analyzing guest sentiment..." />
        </div>
      </DashboardLayout>
    );
  }

  if (error || !summary) {
    return (
      <DashboardLayout>
        <div className="bg-white rounded-xl shadow-sm border border-border p-12 text-center">
          <p className="text-danger font-medium">{error || 'Something went wrong.'}</p>
        </div>
      </DashboardLayout>
    );
  }

  const { overall, monthly, correlation, notable_reviews } = summary;

  return (
    <DashboardLayout>
      <div className="space-y-8">
        {/* Header */}
        <div className="flex justify-between items-start">
          <div>
            <h2 className="text-2xl font-bold tracking-tight text-navy">Guest Sentiment Analysis</h2>
            <p className="text-sm text-text-muted mt-1 italic">TripAdvisor reviews · 686 reviews · 2004–2026</p>
          </div>
          <div className="bg-navy px-3 py-1.5 rounded-lg text-white text-[10px] font-bold tracking-widest uppercase">
            Powered by VADER NLP
          </div>
        </div>

        {/* SECTION 1: KPI Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6">
          <div className="bg-white rounded-xl shadow-sm border border-border p-5 flex items-center">
            <div className="p-3 bg-gold/10 rounded-full mr-4">
              <TrendingUp className="w-6 h-6 text-gold" />
            </div>
            <div>
              <p className="text-xs font-bold text-text-muted uppercase tracking-wider mb-1">Overall Sentiment</p>
              <h3 className="text-2xl font-bold text-navy">{overall.avg_compound_pct}%</h3>
              <p className="text-xs text-text-muted mt-0.5">Avg VADER compound score</p>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-border p-5 flex items-center">
            <div className="p-3 bg-success/10 rounded-full mr-4">
              <ThumbsUp className="w-6 h-6 text-success" />
            </div>
            <div>
              <p className="text-xs font-bold text-text-muted uppercase tracking-wider mb-1">Positive Reviews</p>
              <h3 className="text-2xl font-bold text-navy">{overall.pct_positive}%</h3>
              <p className="text-xs text-text-muted mt-0.5">632 of 686 reviews</p>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-border p-5 flex items-center">
            <div className="p-3 bg-danger/10 rounded-full mr-4">
              <ThumbsDown className="w-6 h-6 text-danger" />
            </div>
            <div>
              <p className="text-xs font-bold text-text-muted uppercase tracking-wider mb-1">Negative Reviews</p>
              <h3 className="text-2xl font-bold text-navy">{overall.pct_negative}%</h3>
              <p className="text-xs text-text-muted mt-0.5">52 of 686 reviews</p>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-border p-5 flex items-center">
            <div className="p-3 bg-gold/10 rounded-full mr-4">
              <Star className="w-6 h-6 text-gold" />
            </div>
            <div>
              <p className="text-xs font-bold text-text-muted uppercase tracking-wider mb-1">Avg Star Rating</p>
              <h3 className="text-2xl font-bold text-navy">3.97 / 5</h3>
              <p className="text-xs text-text-muted mt-0.5">TripAdvisor rating</p>
            </div>
          </div>
        </div>

        {/* SECTION 2: Sentiment Trend Chart */}
        <div className="bg-white rounded-xl shadow-sm border border-border p-6">
          <div className="mb-6 px-1">
            <h3 className="text-lg font-bold text-navy">Monthly Sentiment Score — 2004 to 2026</h3>
            <p className="text-sm text-text-muted">Average VADER compound score per month (only months with reviews shown)</p>
          </div>

          <div className="h-[320px] w-full">
            <ResponsiveContainer width="100%" height="100%">
              <ComposedChart data={trendData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#F1F5F9" />
                <XAxis 
                  dataKey="year_month" 
                  type="category"
                  interval={11}
                  tickFormatter={formatDate}
                  stroke="#94A3B8"
                  fontSize={10}
                />
                <YAxis 
                  domain={[-1, 1]} 
                  tickFormatter={(v) => v.toFixed(1)}
                  stroke="#94A3B8"
                  fontSize={10}
                  label={{ value: 'Sentiment Score', angle: -90, position: 'insideLeft', fontSize: 10, fill: '#94A3B8' }}
                />
                <YAxis 
                  yAxisId="right" 
                  orientation="right" 
                  stroke="#E2E8F0"
                  fontSize={10}
                  label={{ value: 'Reviews', angle: 90, position: 'insideRight', fontSize: 10, fill: '#94A3B8' }}
                />
                <Tooltip 
                  contentStyle={{ borderRadius: '8px', border: '1px solid #E2E8F0', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                  formatter={(value, name) => {
                    if (name === 'avg_compound') return [value.toFixed(3), 'Sentiment Score'];
                    return [value, 'Reviews'];
                  }}
                  labelFormatter={formatDate}
                />
                <ReferenceLine y={0} stroke="#94A3B8" strokeDasharray="4 4" />
                <Bar 
                  dataKey="n_reviews" 
                  fill="#E2E8F0" 
                  yAxisId="right" 
                  opacity={0.4} 
                  radius={[4, 4, 0, 0]}
                />
                <Line 
                  dataKey="avg_compound" 
                  stroke="#C9A84C" 
                  strokeWidth={2.5} 
                  dot={false} 
                  type="monotone" 
                />
              </ComposedChart>
            </ResponsiveContainer>
          </div>

          <div className="mt-6 p-4 bg-[#FEF9EC] border border-gold rounded-xl text-sm text-navy leading-relaxed italic">
            "Overall sentiment is strongly positive at 79.8%. 
             Notable dips: Dec 2025 (20.9%), Jul 2025 (7.7%), Jul 2022 (-97.6%).
             Recent trend shows increased negative reviews in late 2025."
          </div>
        </div>

        {/* SECTION 3: Correlation Analysis Row */}
        <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
          <div className="lg:col-span-2 bg-white rounded-xl shadow-sm border border-border p-6">
            <div className="mb-6">
              <h3 className="text-lg font-bold text-navy">Sentiment vs Occupancy — 2024 Onwards</h3>
              <p className="text-xs text-text-muted mt-0.5">Overlapping period of reviews and historical occupancy</p>
            </div>
            <div className="h-[260px] w-full">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={correlationData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#F1F5F9" />
                  <XAxis 
                    dataKey="year_month" 
                    type="category"
                    tickFormatter={formatDate}
                    stroke="#94A3B8"
                    fontSize={10}
                  />
                  <YAxis 
                    domain={[0, 100]} 
                    stroke="#94A3B8"
                    fontSize={10}
                  />
                  <Tooltip 
                    contentStyle={{ borderRadius: '8px', border: '1px solid #E2E8F0' }}
                    labelFormatter={formatDate}
                  />
                  <Legend />
                  <Line 
                    dataKey="avg_compound_pct" 
                    stroke="#C9A84C" 
                    strokeWidth={2} 
                    name="Sentiment %" 
                    dot={true} 
                  />
                  <Line 
                    dataKey="pct_negative" 
                    stroke="#E53E3E" 
                    strokeWidth={1.5} 
                    strokeDasharray="4 4" 
                    name="% Negative" 
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </div>

          <div className="bg-white rounded-xl shadow-sm border border-border overflow-hidden flex flex-col">
            <div className="bg-navy p-4 text-white">
              <h3 className="text-sm font-bold flex items-center">
                <Activity className="w-4 h-4 mr-2 text-gold" />
                Sentiment → Occupancy Correlation
              </h3>
            </div>
            <div className="p-5 flex-1 space-y-4">
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-text-dark font-medium">Same month</span>
                  <div className="flex items-center">
                    <span className="tabular-nums font-bold text-navy mr-3">r = -0.20</span>
                    <Badge label="Negligible" variant="muted" className="text-[10px]" />
                  </div>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-text-dark font-medium">1-month lag</span>
                  <div className="flex items-center">
                    <span className="tabular-nums font-bold text-navy mr-3">r = -0.18</span>
                    <Badge label="Negligible" variant="muted" className="text-[10px]" />
                  </div>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-text-dark font-medium">2-month lag</span>
                  <div className="flex items-center">
                    <span className="tabular-nums font-bold text-navy mr-3">r = +0.01</span>
                    <Badge label="Negligible" variant="muted" className="text-[10px]" />
                  </div>
                </div>
              </div>

              <div className="p-3 bg-blue-50 border border-blue-100 rounded-lg flex items-start space-x-3">
                <Info className="w-4 h-4 text-blue-500 mt-0.5" />
                <p className="text-[11px] text-blue-700 leading-normal font-medium">
                  Sentiment has a negligible lagged correlation with
                  occupancy (r=-0.18 at 1-month lag). No strong predictive
                  relationship detected.
                </p>
              </div>

              <div className="mt-auto pt-4 flex items-center justify-center space-x-2 text-[10px] text-text-muted italic bg-surface/50 p-2 rounded">
                 <span>Based on 15 overlapping months (Apr 2024 – Dec 2025).</span>
              </div>
            </div>
          </div>
        </div>

        {/* SECTION 4: Review Cards */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
          {/* Positive Reviews */}
          <div className="space-y-4">
            <div className="flex items-center space-x-2 border-l-4 border-success pl-4 mb-4">
              <ThumbsUp className="w-5 h-5 text-success" />
              <h3 className="text-lg font-bold text-navy">Top Positive Reviews</h3>
            </div>
            
            <div className="space-y-4">
              {notable_reviews.top_positive.map((review, idx) => (
                <div key={idx} className="bg-white p-4 rounded-xl border border-border shadow-sm hover:shadow-md transition-shadow">
                  <div className="flex justify-between items-start mb-2">
                    <div className="flex space-x-0.5">
                      {renderStars(review.stars)}
                    </div>
                    <span className="text-xs text-text-muted uppercase font-bold tracking-tight">
                      {formatFullDate(review.date_scraped)}
                    </span>
                  </div>
                  <div className="mb-2">
                    <span className="inline-block bg-success/10 text-success text-[10px] font-bold px-1.5 py-0.5 rounded">
                      +{(review.compound_score * 100).toFixed(0)}% sentiment
                    </span>
                  </div>
                  <p className="text-xs text-text-muted italic leading-relaxed line-clamp-3">
                    "{review.review_text}"
                  </p>
                </div>
              ))}
            </div>
          </div>

          {/* Negative Reviews */}
          <div className="space-y-4">
            <div className="flex items-center space-x-2 border-l-4 border-danger pl-4 mb-4">
              <ThumbsDown className="w-5 h-5 text-danger" />
              <h3 className="text-lg font-bold text-navy">Top Negative Reviews</h3>
            </div>
            
            <div className="space-y-4">
              {notable_reviews.top_negative.map((review, idx) => (
                <div key={idx} className="bg-white p-4 rounded-xl border border-border shadow-sm hover:shadow-md transition-shadow">
                   <div className="flex justify-between items-start mb-2">
                    <div className="flex space-x-0.5">
                      {renderStars(review.stars)}
                    </div>
                    <span className="text-xs text-text-muted uppercase font-bold tracking-tight">
                      {formatFullDate(review.date_scraped)}
                    </span>
                  </div>
                  <div className="mb-2">
                    <span className="inline-block bg-danger/10 text-danger text-[10px] font-bold px-1.5 py-0.5 rounded">
                      {(review.compound_score * 100).toFixed(0)}% sentiment
                    </span>
                  </div>
                  <p className="text-xs text-text-muted italic leading-relaxed line-clamp-3">
                    "{review.review_text}"
                  </p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    </DashboardLayout>
  );
}
