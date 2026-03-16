'use client';

import { useEffect, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { X } from 'lucide-react';

/**
 * SHAP Tooltip - Fixed position Portal overlay
 * Shows detailed explanation for occupancy predictions
 */
export default function ShapTooltip({ 
  date, 
  rate, 
  occ, 
  data, 
  onClose, 
  anchorRect 
}) {
  const tooltipRef = useRef(null);
  const [coords, setCoords] = useState({ top: 0, left: 0 });
  const [isCalculated, setIsCalculated] = useState(false);
  const { top_reasons = [], waterfall = [], base_value = 0 } = data;

  useEffect(() => {
    if (!anchorRect || !tooltipRef.current) return;

    const tooltipHeight = tooltipRef.current.offsetHeight;
    const windowWidth = window.innerWidth;
    const windowHeight = window.innerHeight;

    // Positioning logic per requirement
    let top = anchorRect.bottom + 8;
    let left = Math.min(anchorRect.left, windowWidth - 360);

    // Flip if it overflows the bottom
    if (top + tooltipHeight > windowHeight) {
      top = anchorRect.top - tooltipHeight - 8;
    }

    // Ensure it doesn't overflow top
    top = Math.max(8, top);
    // Ensure it doesn't overflow left
    left = Math.max(8, left);

    setCoords({ top, left });
    setIsCalculated(true);
  }, [anchorRect]);

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

  const formatEffect = (val) => {
    const n = parseFloat(val);
    if (isNaN(n)) return '?pp';
    return n > 0 ? `+${n.toFixed(1)}pp` : `${n.toFixed(1)}pp`;
  };

  const tooltipJSX = (
    <div 
      ref={tooltipRef}
      style={{ 
        top: `${coords.top}px`, 
        left: `${coords.left}px`,
        opacity: isCalculated ? 1 : 0,
        visibility: isCalculated ? 'visible' : 'hidden'
      }}
      className="fixed z-[9999] w-[340px] max-h-[80vh] overflow-y-auto bg-white rounded-xl border border-navy shadow-2xl animate-in fade-in zoom-in duration-200"
    >
      {/* Header */}
      <div className="bg-navy p-3 flex justify-between items-center text-white sticky top-0 z-10">
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
                  {formatEffect(reason.shap_value_pp)}
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
        <div className="space-y-3">
          <h5 className="text-[10px] font-bold text-navy uppercase tracking-wider">How we got there</h5>
          
          {waterfall.length > 0 ? (
            <div className="space-y-2">
              <div className="relative h-6 flex rounded overflow-hidden bg-surface border border-border">
                {waterfall.map((segment, idx) => {
                  const width = Math.abs(segment.weight_pct);
                  let bgColor = 'bg-navy';
                  if (segment.type === 'base') bgColor = 'bg-navy';
                  else if (segment.value > 0 || segment.weight_pct > 0) bgColor = 'bg-success';
                  else bgColor = 'bg-danger';

                  return (
                    <div 
                      key={idx}
                      title={`${segment.feature || segment.type}: ${segment.value || 0}`}
                      className={`${bgColor} h-full border-r border-white/20 last:border-0`}
                      style={{ width: `${width}%` }}
                    />
                  );
                })}
              </div>
              
              <div className="flex justify-between items-center text-[10px] text-text-muted font-medium px-0.5">
                <span>Base {(base_value * 100).toFixed(0)}%</span>
                <span className="font-bold text-navy">Final {occ}%</span>
              </div>
            </div>
          ) : (
            <div className="bg-surface/50 p-2 rounded text-[11px] text-center text-text-muted border border-border">
              Base: {(base_value * 100).toFixed(0)}% → Final: {occ}%
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="pt-2 text-[8px] text-center text-text-muted border-t border-border/50">
          Powered by SHAP · GBM+RF ensemble
        </div>
      </div>
    </div>
  );

  return createPortal(tooltipJSX, document.body);
}

