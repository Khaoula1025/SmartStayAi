import { ArrowDownIcon, ArrowUpIcon, MinusIcon } from 'lucide-react';
import { cn } from '../../lib/utils';

export default function KpiCard({ title, value, subtitle, icon: Icon, trend, color = 'navy' }) {
  const getTrendIcon = () => {
    if (trend === 'up') return <ArrowUpIcon className="w-4 h-4 text-success" />;
    if (trend === 'down') return <ArrowDownIcon className="w-4 h-4 text-danger" />;
    if (trend === 'neutral') return <MinusIcon className="w-4 h-4 text-text-muted" />;
    return null;
  };

  const getIconBg = () => {
    switch (color) {
      case 'gold': return 'bg-gold/10 text-gold';
      case 'success': return 'bg-success/10 text-success';
      case 'danger': return 'bg-danger/10 text-danger';
      case 'warning': return 'bg-warning/10 text-warning';
      case 'navy':
      default: return 'bg-navy/10 text-navy';
    }
  };

  return (
    <div className="bg-white rounded-xl p-5 shadow-sm border border-border flex flex-col h-full">
      <div className="flex items-center justify-between mb-4">
        <h3 className="text-sm font-medium text-text-muted">{title}</h3>
        {Icon && (
          <div className={cn("p-2 rounded-full", getIconBg())}>
            <Icon className="w-5 h-5" />
          </div>
        )}
      </div>
      
      <div className="mt-auto">
        <div className="text-3xl font-bold text-text-dark tabular-nums tracking-tight">
          {value}
        </div>
        
        <div className="flex items-center mt-2">
          {trend && (
            <div className="flex items-center mr-2">
              {getTrendIcon()}
            </div>
          )}
          {subtitle && (
            <p className="text-sm text-text-muted">{subtitle}</p>
          )}
        </div>
      </div>
    </div>
  );
}
