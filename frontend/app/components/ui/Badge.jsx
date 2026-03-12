import { cn } from '../../lib/utils';

export default function Badge({ label, variant = 'muted', className }) {
  const getVariantStyles = () => {
    switch (variant) {
      case 'success':
        return 'bg-success/15 text-success ring-1 ring-inset ring-success/20';
      case 'warning':
        return 'bg-warning/15 text-warning ring-1 ring-inset ring-warning/20';
      case 'danger':
        return 'bg-danger/15 text-danger ring-1 ring-inset ring-danger/20';
      case 'info':
        return 'bg-navy/15 text-navy ring-1 ring-inset ring-navy/20';
      case 'gold':
        return 'bg-gold/15 text-gold-dark ring-1 ring-inset ring-gold/20';
      case 'muted':
      default:
        return 'bg-gray-100 text-text-muted ring-1 ring-inset ring-gray-500/10';
    }
  };

  return (
    <span
      className={cn(
        'inline-flex items-center rounded-full px-2.5 py-0.5 text-xs font-semibold uppercase tracking-wider',
        getVariantStyles(),
        className
      )}
    >
      {label}
    </span>
  );
}
