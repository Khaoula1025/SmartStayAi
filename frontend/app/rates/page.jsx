'use client';

import { useState, useEffect, useMemo } from 'react';
import { Filter, Check, Edit2, Minus, Calendar, DollarSign, Activity, Settings2, Download } from 'lucide-react';
import { getPredictions, getRateDecisions, postRateDecision } from '../lib/api';
import { formatOcc, formatRate, formatShortDate, formatDate, tierColor } from '../lib/format';
import DashboardLayout from '../components/layout/DashboardLayout';
import Badge from '../components/ui/Badge';
import AlertBanner from '../components/ui/AlertBanner';
import LoadingSpinner from '../components/ui/LoadingSpinner';
import Modal from '../components/ui/Modal';

export default function RatesPage() {
  const [data, setData] = useState([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Filters
  const today = new Date();
  const plus60 = new Date();
  plus60.setDate(today.getDate() + 60);
  
  const [dateFrom, setDateFrom] = useState(today.toISOString().split('T')[0]);
  const [dateTo, setDateTo] = useState(plus60.toISOString().split('T')[0]);
  const [actionFilter, setActionFilter] = useState('all');
  
  // Modal State
  const [isOverrideModalOpen, setIsOverrideModalOpen] = useState(false);
  const [overrideDate, setOverrideDate] = useState(null);
  const [overrideData, setOverrideData] = useState(null);
  const [overrideRateStr, setOverrideRateStr] = useState('');
  const [overrideReason, setOverrideReason] = useState('');
  const [isSubmitting, setIsSubmitting] = useState(false);

  const loadData = async (from, to) => {
    setIsLoading(true);
    setError(null);
    try {
      const [predictionsRes, decisionsRes] = await Promise.all([
        getPredictions(from, to).catch(() => []),
        getRateDecisions(from, to).catch(() => [])
      ]);
      
      // Merge decisions into predictions
      const decisionsMap = {};
      decisionsRes.forEach(d => {
        decisionsMap[d.date] = d;
      });
      
      const merged = (predictionsRes || []).map(p => ({
        ...p,
        decision: decisionsMap[p.date] || null
      }));
      
      setData(merged);
    } catch (err) {
      console.error('Failed to load rates data:', err);
      setError('Could not load rate recommendations. Please try again later.');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadData(dateFrom, dateTo);
  }, []);

  const handleApplyFilters = () => {
    loadData(dateFrom, dateTo);
  };

  const filteredData = useMemo(() => {
    if (actionFilter === 'all') return data;
    
    return data.filter(item => {
      const hasDecision = !!item.decision;
      if (actionFilter === 'pending') return !hasDecision;
      if (actionFilter === 'accept') return hasDecision && item.decision.action === 'accept';
      if (actionFilter === 'override') return hasDecision && item.decision.action === 'override';
      if (actionFilter === 'ignore') return hasDecision && item.decision.action === 'ignore';
      return true;
    });
  }, [data, actionFilter]);

  // Summary Stats
  const summaryStats = useMemo(() => {
    let accepted = 0;
    let overridden = 0;
    let ignored = 0;
    let pending = 0;
    
    data.forEach(item => {
      if (!item.decision) {
        pending++;
      } else {
        if (item.decision.action === 'accept') accepted++;
        if (item.decision.action === 'override') overridden++;
        if (item.decision.action === 'ignore') ignored++;
      }
    });
    
    return { accepted, overridden, ignored, pending };
  }, [data]);

  // Handle Rate Decisions
  const handleDecision = async (dateStr, actionData) => {
    // Determine the relevant row
    const targetItem = data.find(i => i.date === dateStr);
    if (!targetItem) return;
    
    const previousDecision = targetItem.decision;
    const optimisticDecision = {
      date: dateStr,
      action: actionData.action,
      final_rate: actionData.final_rate,
      override_reason: actionData.override_reason || '',
      created_at: new Date().toISOString()
    };
    
    // Optimistic Update
    setData(prev => prev.map(item => 
      item.date === dateStr ? { ...item, decision: optimisticDecision } : item
    ));
    
    try {
      await postRateDecision({
        date: dateStr,
        ...actionData,
        recommended_rate: targetItem.recommended_rate
      });
      // Success, leave optimistic update
    } catch (err) {
      console.error('Failed to save decision:', err);
      // Revert optimism
      setData(prev => prev.map(item => 
        item.date === dateStr ? { ...item, decision: previousDecision } : item
      ));
      alert(`Failed to save decision for ${dateStr}. Please try again.`);
    }
  };

  const onAccept = (dateStr, recommendedRate) => {
    handleDecision(dateStr, {
      action: 'accept',
      final_rate: recommendedRate
    });
  };

  const onIgnore = (dateStr) => {
    handleDecision(dateStr, {
      action: 'ignore'
    });
  };

  const openOverrideModal = (item) => {
    setOverrideDate(item.date);
    setOverrideData(item);
    setOverrideRateStr(item.recommended_rate.toString());
    setOverrideReason('');
    setIsOverrideModalOpen(true);
  };

  const closeOverrideModal = () => {
    setIsOverrideModalOpen(false);
    setOverrideDate(null);
    setOverrideData(null);
  };

  const submitOverride = async (e) => {
    e.preventDefault();
    if (!overrideRateStr || !overrideReason.trim()) return;
    
    const rate = parseFloat(overrideRateStr);
    if (isNaN(rate)) return;

    setIsSubmitting(true);
    
    // Using handleDecision handles the optimistic update + api call, but it's async under the hood
    // and catches its own errors. We just assume it will try or revert.
    handleDecision(overrideDate, {
      action: 'override',
      final_rate: rate,
      override_reason: overrideReason.trim()
    });
    
    setIsSubmitting(false);
    closeOverrideModal();
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
              <label className="text-xs font-semibold text-text-muted uppercase tracking-wider">Filter State</label>
              <select
                value={actionFilter}
                onChange={(e) => setActionFilter(e.target.value)}
                className="block w-full rounded-md border-0 py-2 pl-3 pr-8 text-text-dark shadow-sm ring-1 ring-inset ring-border focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6 bg-white capitalize"
              >
                <option value="all">All Dates</option>
                <option value="pending">Pending Only</option>
                <option value="accept">Accepted</option>
                <option value="override">Overridden</option>
                <option value="ignore">Ignored</option>
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
            <LoadingSpinner text="Loading rate recommendations..." />
          </div>
        ) : (
          <>
            {/* Summary Stat Pills */}
            <div className="flex flex-wrap gap-4">
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-success/20 flex flex-col justify-center min-w-[140px]">
                <p className="text-xs text-text-muted font-semibold uppercase tracking-wider mb-1 flex items-center text-success"><Check className="w-3 h-3 mr-1" /> Accepted</p>
                <p className="text-xl font-bold text-text-dark tabular-nums">{summaryStats.accepted}</p>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-warning/30 flex flex-col justify-center min-w-[140px]">
                <p className="text-xs text-text-muted font-semibold uppercase tracking-wider mb-1 flex items-center text-warning-dark"><Edit2 className="w-3 h-3 mr-1" /> Overridden</p>
                <p className="text-xl font-bold text-text-dark tabular-nums">{summaryStats.overridden}</p>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border border-border flex flex-col justify-center min-w-[140px]">
                <p className="text-xs text-text-muted font-semibold uppercase tracking-wider mb-1 flex items-center"><Minus className="w-3 h-3 mr-1" /> Ignored</p>
                <p className="text-xl font-bold text-text-dark tabular-nums">{summaryStats.ignored}</p>
              </div>
              <div className="bg-white px-4 py-3 rounded-lg shadow-sm border-l-4 border-l-navy border border-border flex flex-col justify-center min-w-[140px]">
                <p className="text-xs text-text-muted font-semibold uppercase tracking-wider mb-1">Pending Actions</p>
                <p className="text-xl font-bold text-navy tabular-nums">{summaryStats.pending}</p>
              </div>
            </div>

            {/* Data Table */}
            <div className="bg-white rounded-xl shadow-sm border border-border overflow-hidden">
              <div className="p-4 border-b border-border bg-surface/50">
                <h3 className="text-lg font-bold text-navy">Rate Recommendations & Decisions</h3>
              </div>
              
              <div className="overflow-x-auto">
                {filteredData.length === 0 ? (
                  <div className="p-16 text-center text-text-muted">
                    No rate recommendations found for the selected period and filters.
                  </div>
                ) : (
                  <table className="w-full text-sm text-left">
                    <thead className="bg-white border-b border-border text-text-muted">
                      <tr>
                        <th className="px-5 py-4 font-semibold w-[120px]">Date</th>
                        <th className="px-5 py-4 font-semibold w-[80px]">Day</th>
                        <th className="px-5 py-4 font-semibold text-right">Predicted Occ</th>
                        <th className="px-5 py-4 font-semibold text-right">Recommended Rate</th>
                        <th className="px-5 py-4 font-semibold text-center">Tier</th>
                        <th className="px-5 py-4 font-semibold w-[320px]">Decision</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-border">
                      {filteredData.map((row) => {
                        const d = new Date(row.date);
                        const dayName = isNaN(d) ? '' : d.toLocaleDateString('en-GB', { weekday: 'short' });
                        
                        return (
                          <tr key={row.date} className="hover:bg-surface/30 transition-colors h-16 group">
                            <td className="px-5 py-2 font-medium text-text-dark whitespace-nowrap">
                              {formatShortDate(row.date)}
                            </td>
                            <td className="px-5 py-2 text-text-muted">
                              {dayName}
                            </td>
                            <td className="px-5 py-2 text-right tabular-nums">
                              {formatOcc(row.predicted_occ)}
                            </td>
                            <td className="px-5 py-2 text-right tabular-nums">
                              <span className="font-semibold text-text-dark bg-surface px-2 py-1 rounded inline-block">
                                {formatRate(row.recommended_rate)}
                              </span>
                            </td>
                            <td className="px-5 py-2 text-center">
                              <Badge 
                                label={row.rate_tier || 'Unknown'} 
                                style={{ 
                                  backgroundColor: `${tierColor(row.rate_tier)}15`, 
                                  color: tierColor(row.rate_tier)
                                }}
                              />
                            </td>
                            <td className="px-5 py-2">
                              {row.decision ? (
                                <div className="flex items-center gap-2">
                                  {row.decision.action === 'accept' && (
                                    <Badge variant="success" label="Accepted" className="w-[85px] justify-center" />
                                  )}
                                  {row.decision.action === 'override' && (
                                    <>
                                      <Badge variant="warning" label="Overridden" className="w-[85px] justify-center" />
                                      <span className="text-xs text-text-muted tabular-nums">
                                        Was {formatRate(row.recommended_rate)} → <span className="text-text-dark font-bold pl-0.5">{formatRate(row.decision.final_rate)}</span>
                                      </span>
                                    </>
                                  )}
                                  {row.decision.action === 'ignore' && (
                                    <Badge variant="muted" label="Ignored" className="w-[85px] justify-center" />
                                  )}
                                </div>
                              ) : (
                                <div className="flex items-center gap-1.5 opacity-0 group-hover:opacity-100 transition-opacity">
                                  <button
                                    onClick={() => onAccept(row.date, row.recommended_rate)}
                                    className="flex items-center justify-center px-2 py-1.5 bg-success/10 text-success hover:bg-success hover:text-white rounded text-xs font-semibold tracking-wide uppercase transition-colors"
                                  >
                                    <Check className="w-3.5 h-3.5 mr-1" />
                                    Accept
                                  </button>
                                  <button
                                    onClick={() => openOverrideModal(row)}
                                    className="flex items-center justify-center px-2 py-1.5 bg-warning/10 text-warning-dark hover:bg-warning hover:text-white rounded text-xs font-semibold tracking-wide uppercase transition-colors"
                                  >
                                    <Edit2 className="w-3.5 h-3.5 mr-1" />
                                    Override
                                  </button>
                                  <button
                                    onClick={() => onIgnore(row.date)}
                                    className="flex items-center justify-center px-2 py-1.5 bg-gray-100 text-text-muted hover:bg-gray-200 hover:text-text-dark rounded text-xs font-semibold tracking-wide uppercase transition-colors"
                                  >
                                    <Minus className="w-3.5 h-3.5 mr-1" />
                                    Ignore
                                  </button>
                                </div>
                              )}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                )}
              </div>
            </div>
          </>
        )}
      </div>

      {/* Override Modal */}
      <Modal
        isOpen={isOverrideModalOpen}
        onClose={closeOverrideModal}
        title={`Override Rate — ${overrideDate ? formatDate(overrideDate) : ''}`}
        footer={
          <>
            <button
              onClick={closeOverrideModal}
              className="px-4 py-2 bg-transparent text-text-muted hover:text-text-dark hover:bg-gray-100 font-medium rounded-md transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={submitOverride}
              form="override-form"
              disabled={isSubmitting || !overrideRateStr || !overrideReason.trim()}
              className="px-4 py-2 bg-navy text-white hover:bg-navy-light font-medium rounded-md transition-colors disabled:opacity-50 flex items-center"
            >
              {isSubmitting ? (
                <>
                  <Activity className="w-4 h-4 mr-2 animate-spin" />
                  Saving...
                </>
              ) : (
                'Confirm Override'
              )}
            </button>
          </>
        }
      >
        <form id="override-form" onSubmit={submitOverride} className="space-y-4">
          <div className="bg-surface p-4 rounded-lg border border-border mb-6">
            <p className="text-sm font-medium text-text-muted uppercase tracking-wider mb-1">SmartStay Recommended Rate</p>
            <p className="text-2xl font-bold text-navy tabular-nums">{overrideData ? formatRate(overrideData.recommended_rate) : '-'}</p>
          </div>
          
          <div>
            <label htmlFor="rateInput" className="block text-sm font-bold text-text-dark mb-1">
              Your Rate £
            </label>
            <div className="relative rounded-md shadow-sm">
              <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3">
                <span className="text-text-muted sm:text-sm">£</span>
              </div>
              <input
                type="number"
                name="rateInput"
                id="rateInput"
                step="0.01"
                min={overrideData && overrideData.floor_price ? overrideData.floor_price : 0}
                className="block w-full rounded-md border-0 py-2.5 pl-8 pr-12 text-text-dark ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6"
                placeholder="0.00"
                value={overrideRateStr}
                onChange={(e) => setOverrideRateStr(e.target.value)}
                required
              />
              <div className="pointer-events-none absolute inset-y-0 right-0 flex items-center pr-3">
                <span className="text-text-muted sm:text-sm" id="price-currency">
                  GBP
                </span>
              </div>
            </div>
            {overrideData && overrideData.floor_price && (
              <p className="mt-1 text-xs text-text-muted">Minimum floor price: {formatRate(overrideData.floor_price)}</p>
            )}
          </div>
          
          <div>
            <label htmlFor="reasonTextarea" className="block text-sm font-bold text-text-dark mb-1">
              Reason for override
            </label>
            <textarea
              id="reasonTextarea"
              rows={3}
              placeholder="e.g. Discussed with GM, recent corporate booking inquiry..."
              className="block w-full rounded-md border-0 py-2 px-3 text-text-dark shadow-sm ring-1 ring-inset ring-border placeholder:text-text-muted focus:ring-2 focus:ring-inset focus:ring-navy sm:text-sm sm:leading-6 resize-none"
              value={overrideReason}
              onChange={(e) => setOverrideReason(e.target.value)}
              required
            />
          </div>
        </form>
      </Modal>
    </DashboardLayout>
  );
}
