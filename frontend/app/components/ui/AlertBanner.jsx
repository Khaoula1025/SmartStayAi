'use client';

import { useState } from 'react';
import { AlertCircle, AlertTriangle, Info, X } from 'lucide-react';
import { cn } from '../../lib/utils';

export default function AlertBanner({ message, type = 'info', dismissible = true }) {
  const [isVisible, setIsVisible] = useState(true);

  if (!isVisible) return null;

  const getStyles = () => {
    switch (type) {
      case 'error':
        return {
          bg: 'bg-danger/10',
          border: 'border-danger/20',
          text: 'text-danger',
          icon: <AlertCircle className="w-5 h-5 text-danger" />,
        };
      case 'warning':
        return {
          bg: 'bg-warning/10',
          border: 'border-warning/20',
          text: 'text-warning-dark', // Need darker text for contrast, warning is yellow
          icon: <AlertTriangle className="w-5 h-5 text-warning" />,
        };
      case 'info':
      default:
        return {
          bg: 'bg-navy/10',
          border: 'border-navy/20',
          text: 'text-navy',
          icon: <Info className="w-5 h-5 text-navy" />,
        };
    }
  };

  const styles = getStyles();

  return (
    <div className={cn('rounded-lg p-4 border flex items-start mb-4', styles.bg, styles.border)}>
      <div className="flex-shrink-0 mr-3 mt-0.5">{styles.icon}</div>
      <div className={cn('flex-1 text-sm font-medium', styles.text)}>
        {message}
      </div>
      {dismissible && (
        <button
          onClick={() => setIsVisible(false)}
          className={cn('ml-auto flex-shrink-0 rounded-md p-1 hover:bg-black/5 focus:outline-none focus:ring-2 focus:ring-offset-2', styles.text)}
        >
          <span className="sr-only">Dismiss</span>
          <X className="w-4 h-4" />
        </button>
      )}
    </div>
  );
}
