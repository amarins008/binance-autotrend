# Dashboard UI Redesign Plan

## Overview
Full rewrite of the Autotrend Trading Dashboard from pixel-art "Virtual Office" theme to a modern, clean card-based UI.

## Design Principles
1. **Clean & Minimal** - Remove pixel-art/chibi, use modern card-based layout
2. **Information Density** - Show more data in less space without clutter
3. **Visual Hierarchy** - Clear typography scale, color coding for status
4. **Responsive** - Mobile-first, works on all screen sizes
5. **Dark Theme** - Keep dark theme but with refined colors

## New Design System

### Colors
```css
:root {
  --bg-primary: #0a0f1a;
  --bg-secondary: #111827;
  --bg-card: rgba(17, 24, 39, 0.8);
  --bg-card-hover: rgba(30, 41, 59, 0.9);
  --border: rgba(75, 85, 99, 0.3);
  --border-active: rgba(56, 189, 248, 0.5);
  
  --text-primary: #f9fafb;
  --text-secondary: #9ca3af;
  --text-muted: #6b7280;
  
  --accent: #3b82f6;
  --accent-light: #60a5fa;
  --success: #10b981;
  --warning: #f59e0b;
  --danger: #ef4444;
  
  --gradient-accent: linear-gradient(135deg, #3b82f6, #8b5cf6);
  --gradient-success: linear-gradient(135deg, #10b981, #059669);
  --gradient-danger: linear-gradient(135deg, #ef4444, #dc2626);
}
```

### Typography
- **Headers**: DM Sans 700, 18-24px
- **Body**: DM Sans 400-500, 13-14px
- **Data**: JetBrains Mono 500, 12-14px
- **Badges**: DM Sans 600, 10-11px

### Spacing Scale
- xs: 4px
- sm: 8px
- md: 12px
- lg: 16px
- xl: 20px
- 2xl: 24px

### Border Radius
- sm: 6px
- md: 8px
- lg: 12px
- xl: 16px
- full: 9999px

## New Layout Structure

### Desktop (>1180px)
```
┌─────────┬────────────────────────────────────────┐
│ Sidebar │              Main Content               │
│  260px  │                                        │
│         │  ┌─────────────────────────────────┐   │
│ Brand   │  │     Top Stats Bar (4 cards)     │   │
│ Nav     │  └─────────────────────────────────┘   │
│ Status  │  ┌──────────────┬──────────────────┐   │
│ KPIs    │  │  Agent Grid  │   Entry Pipeline │   │
│ Config  │  │  (2x5 grid)  │   + Positions    │   │
│ Controls│  │              │                  │   │
│         │  └──────────────┴──────────────────┘   │
│         │  ┌─────────────────────────────────┐   │
│         │  │       Supervisor Review          │   │
│         │  └─────────────────────────────────┘   │
│         │  ┌──────────────┬──────────────────┐   │
│         │  │ Decision Log │    Learning       │   │
│         │  └──────────────┴──────────────────┘   │
└─────────┴────────────────────────────────────────┘
```

### Mobile (<760px)
```
┌─────────────────────────────┐
│         Sidebar             │
│  Brand | Nav | Status       │
├─────────────────────────────┤
│      Top Stats Bar          │
├─────────────────────────────┤
│      Agent Grid (scroll)    │
├─────────────────────────────┤
│      Entry Pipeline         │
├─────────────────────────────┤
│      Positions              │
├─────────────────────────────┤
│      Supervisor Review      │
├─────────────────────────────┤
│      Decision Log           │
├─────────────────────────────┤
│      Learning               │
└─────────────────────────────┘
```

## Component Redesign

### 1. Sidebar (260px)
- Clean brand section with logo
- Tab navigation (Office | Logs)
- Status panel with colored indicators
- KPI cards (Win Rate, PnL)
- Configuration controls
- Connection settings

### 2. Top Stats Bar
- 4 metric cards in a row
- Each card: icon + value + label + trend indicator
- Cards: Backend Status, Bot Status, Active Agents, Today's PnL

### 3. Agent Grid (replaces chibi office)
- 2x5 grid of agent cards
- Each card shows:
  - Agent name + role
  - Status badge (Active/Idle/Done/Blocked)
  - Last action text
  - Run count
- Clean card design with subtle borders
- Color-coded status indicators

### 4. Entry Pipeline
- Horizontal flow of gate chips
- Each gate: icon + name + status (pass/fail)
- Compact design with clear visual feedback

### 5. Positions Panel
- Card with list of open positions
- Each position: symbol + side badge + uPnL + leverage
- Clean table-like layout

### 6. Supervisor Review
- Card with severity badge
- List of issues with agent name + title + detail
- Clean, scannable layout

### 7. Decision Log
- Scrollable log panel
- Timestamped entries with color coding
- Compact monospace font

### 8. Learning Panel
- Stats grid (Last train, Scanned, Promoted, Threshold)
- Promoted symbols list
- Walk-forward results
- Auto-tune recommendations

## Files to Modify

### 1. `index.html`
- Complete rewrite of HTML structure
- Remove all chibi/office CSS (~2000 lines)
- New modern CSS design system
- Keep all JavaScript logic intact (just update element IDs)

### 2. `app.js`
- Update element references in `ui` object
- Update render functions for new HTML structure
- Keep all API calls and business logic unchanged

### 3. `styles.css`
- Complete rewrite to match new design
- Remove pixel-art styles
- Add modern utility classes

## Implementation Steps

### Phase 1: CSS Design System
1. Create new CSS variables
2. Build component styles (cards, badges, buttons, inputs)
3. Create layout grid system
4. Add responsive breakpoints

### Phase 2: HTML Structure
1. Rewrite sidebar HTML
2. Create new main content layout
3. Build agent card components
4. Create pipeline, positions, supervisor sections
5. Add log and learning panels

### Phase 3: JavaScript Updates
1. Update `ui` object with new element IDs
2. Update `renderHermesKanban()` for new agent cards
3. Update `renderOpenPositions()` for new layout
4. Update `renderSymbolProfileSummary()` for new design
5. Update `renderHermesSupervisor()` for new cards
6. Update `paintLearning()` for new layout

### Phase 4: Testing
1. Test on desktop (1920x1080)
2. Test on tablet (768px)
3. Test on mobile (375px)
4. Verify all interactions work
5. Check responsive breakpoints

## Estimated Changes
- **CSS**: ~3500 lines (remove ~2500 lines of pixel-art, add ~1000 lines of modern styles)
- **HTML**: ~800 lines (down from ~3500 lines)
- **JavaScript**: ~200 lines changed (element references + render functions)

## Risk Assessment
- **High**: Full rewrite means potential for breaking changes
- **Mitigation**: Keep all API calls and business logic unchanged
- **Mitigation**: Test each component thoroughly
- **Mitigation**: Keep backup of original files

## Success Criteria
1. Modern, clean card-based design
2. No pixel-art or chibi elements
3. Responsive on all screen sizes
4. All functionality preserved
5. Better information density
6. Improved visual hierarchy
