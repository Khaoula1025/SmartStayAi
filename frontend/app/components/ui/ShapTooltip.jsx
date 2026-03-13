'use client';

import { useEffect, useRef } from 'react';
import { X } from 'lucide-react';

export default function ShapTooltip({ 
  date, 
  rate, 
  occ, 
  data, 
  onClose, 
  anchorRect 
}) {
  const tooltipRef = useRef(null);
  const { top_reasons = [], waterfall = [] } = data;

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (tooltipRef.current && !tooltipRef.current.contains(event.target)) {
        onClose();
      }
    };

    const handleEscape = (event) => {
      if (event.key === 'Escape') {
        onClose();
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    document.addEventListener('keydown', handleEscape);

    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [onClose]);

  if (!anchorRect) return null;

  // Positioning logic: centered below the button
  const top = anchorRect.bottom + window.scrollY + 8;
  const left = anchorRect.left + window.scrollX - 160 + (anchorRect.width / 2);

  const formatEffect = (val) => {
    const num = parseFloat(val);
    return num > 0 ? `+${num.toFixed(1)}pp` : `${num.toFixed(1)}pp`;
  };

  return (
    <div 
      ref={tooltipRef}
      style={{ top: `${top}px`, left: `${left}px` }}
      className="fixed z-50 w-[320px] bg-white rounded-xl border border-navy shadow-xl overflow-hidden animate-in fade-in zoom-in duration-200"
    >
      {/* Header */}
      <div className="bg-navy p-3 flex justify-between items-center text-white">
        <h4 className="font-bold text-sm">Why £{rate} on {date}?</h4>
        <button onClick={onClose} className="hover:bg-white/10 rounded-full p-1 transition-colors">
          <X size={16} />
        </button>
      </div>

      <div className="p-4 space-y-4">
        {/* Predicted Occ */}
        <div className="text-center">
          <p className="text-gold font-bold text-lg">Predicted occupancy: {occ}%</p>
        </div>

        <hr className="border-border" />

        {/* Top Reasons */}
        <div className="space-y-3">
          <h5 className="text-[10px] font-bold text-navy uppercase tracking-wider">Top 3 reasons</h5>
          {top_reasons.slice(0, 3).map((reason, idx) => (
            <div key={idx} className="space-y-1">
              <div className="flex items-center justify-between text-xs">
                <div className={`px-1.5 py-0.5 rounded font-bold min-w-[50px] text-center ${reason.direction === 'up' ? 'bg-success/10 text-success' : 'bg-danger/10 text-danger'}`}>
                  {formatEffect(reason.impact)}
                </div>
                <div className="flex-1 px-2 font-semibold text-text-dark truncate">
                  {reason.feature}
                </div>
                <div className="text-text-muted">
                  {reason.value}
                </div>
              </div>
              <p className="text-[10px] text-text-muted italic leading-tight pl-[58px]">
                {reason.explanation}
              </p>
            </div>
          ))}
        </div>

        <hr className="border-border" />

        {/* Waterfall */}
        <div className="space-y-2">
          <h5 className="text-[10px] font-bold text-navy uppercase tracking-wider">How we got there</h5>
          
          <div className="relative h-6 flex rounded overflow-hidden bg-surface">
            {waterfall.map((segment, idx) => {
              const width = Math.abs(segment.weight_pct);
              let bgColor = 'bg-navy'; // fallback for base
              if (segment.type === 'base') bgColor = 'bg-navy';
              else if (segment.weight_pct > 0) bgColor = 'bg-success';
              else bgColor = 'bg-danger';

              return (
                <div 
                  key={idx}
                  className={`${bgColor} h-full border-r border-white last:border-0`}
                  style={{ width: `${width}%` }}
                />
              );
            })}
          </div>
          
          <div className="flex justify-between items-center text-[10px] text-text-muted px-0.5">
            <span>Base {waterfall.find(s => s.type === 'base')?.value}%</span>
            <span className="font-bold text-navy">Final {occ}%</span>
          </div>
        </div>

        {/* Footer */}
        <div className="pt-2 text-[8px] text-center text-text-muted border-t border-border/50">
          Powered by SHAP · GBM+RF ensemble
        </div>
      </div>
    </div>
  );
}
