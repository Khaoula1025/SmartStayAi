import { Loader2 } from 'lucide-react';
import { cn } from '../../lib/utils';

export default function LoadingSpinner({ className, text = 'Loading...' }) {
  return (
    <div className={cn('flex flex-col items-center justify-center p-12 w-full h-full', className)}>
      <div className="relative">
        <Loader2 className="w-12 h-12 text-gold animate-spin" />
        <div className="absolute inset-0 border-4 border-navy-light opacity-20 rounded-full"></div>
      </div>
      {text && (
        <p className="mt-4 text-sm font-medium text-text-muted animate-pulse">{text}</p>
      )}
    </div>
  );
}
