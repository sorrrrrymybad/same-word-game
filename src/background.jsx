// Author: Sorrymybad
import { useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import GridScan from './components/GridScan';

function Background() {
  const [theme, setTheme] = useState(document.documentElement.dataset.theme);
  const [reducedMotion, setReducedMotion] = useState(matchMedia('(prefers-reduced-motion: reduce)').matches);

  useEffect(() => {
    const observer = new MutationObserver(() => setTheme(document.documentElement.dataset.theme));
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    const motion = matchMedia('(prefers-reduced-motion: reduce)');
    const updateMotion = () => setReducedMotion(motion.matches);
    motion.addEventListener('change', updateMotion);
    return () => {
      observer.disconnect();
      motion.removeEventListener('change', updateMotion);
    };
  }, []);

  const colors = getComputedStyle(document.documentElement);
  return <GridScan
    key={String(reducedMotion)}
    linesColor={colors.getPropertyValue('--grid-lines').trim()}
    scanColor={colors.getPropertyValue('--grid-scan').trim()}
    lightMode={theme === 'light'}
    cells={8}
    depth={3}
    scanDuration={2.4}
    scanDelay={1.6}
    clickEffect="ping"
    dpr={1}
  />;
}

createRoot(document.getElementById('grid-background')).render(<Background />);
