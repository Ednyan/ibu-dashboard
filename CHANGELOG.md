# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.2.0]

### Improved

- Reorganized html files into blocks
- Much more mobile friendly UI
- Minify html/css/js responses for faster load times

### Changed

- Switched from custom css to tailwind
- Switched from plotly to apexcharts for graphs

### Added

- Point gain over the last day on the team overview section
- Start to the member page

---

## [1.1.3] - 2026-01-18

### Changed

- Switched the docker image to Debian
- Removed some dead code

---

## [1.1.2] - 2026-01-15

### Added

- Link to the source code

---

## [1.1.1] - 2026-01-14

### Improved

- Stats overview is now at a fixed location and as some visual enhancements

---

## [1.1.0] - 2025-12-28

### Added

- Docker support

### Changed

- Rename instances of `CSV` to `data`
- Rewrote some functions in rust
- Use uwsgi as the webserver
- Rename Daily Statistics to Statistics Overview
- Get top 10 performers instead of top 5

### Improved

- Stats Overview loads much faster

---

## [1.0.3] - 2025-08-31

### Added

- New time intervals (90 days, 180 days) for Trends
- New time intervals (90 days, 180 days) for Member Points
- Webhook (IBU Assistant) notifications for passed, failed, and non-compliant members

---

## [1.0.2] - 2025-08-31

### Added

- Member cards with redirect button to SheepIt profile

### Fixed

- Search functionality on Data Series (Trends and Predictions page)

### Improved

- Homepage UI flexibility

---

## [1.0.1] - 2025-08-15

### Added

- Member search on tracking page
- Tri-state milestone overrides (None / Pass / Fail)
- Milestone badges clarifying Forgiven vs Denied states
- Admin notifications manager with per-recipient and per-email preferences

### Fixed

- Admin page functionality

---

## [1.0.0] - 2025-08-13

### Added

- Team and member trends page with multi-series selection
- Time aggregation options (daily, weekly, monthly, yearly)
- Value modes (cumulative vs interval)
- Vertical date range picker with quick-select ranges
- Search with shift-click range selection and enable/disable-all controls
- Team metric comparisons (points, members, 90d, 180d)
- Prediction modes (linear regression and moving average, configurable)
- Hover mode selector (separate, unified, closest)

---

## [0.4.2] - 2025-08-08

### Fixed

- Notification system issues
- Pie chart rendering issues

---

## [0.4.1] - 2025-07-26

### Added

- Interactive CSV download with date range selection
- Custom datepicker modal with available data highlighting

---

## [0.4.0] - 2025-07-26

### Added

- Complete member probation tracking with milestone validation
- Post-probation compliance monitoring
- Interactive member filtering system
- Automated email notifications for probation failures
- Anti-spam limits for CSV-triggered notifications

### Improved

- Horizontal navigation design

---

## [0.3.0] - 2025-07-22

### Added

- jQuery UI datepicker with CSV highlighting

### Improved

- Professional color scheme redesign
- Responsive navigation
- Chart performance

---

## [0.2.1] - 2025-07-05

### Improved

- Embedded fonts in SVG files
- Logo and graphic load performance
- Cross-platform SVG compatibility

---

## [0.2.0] - 2025-07-04

### Added

- Interactive pie charts using Plotly
- Custom date range selection
- Automatic data refresh system

### Changed

- Complete UI redesign

---

## [0.1.0] - 2025-07-01

### Added

- Basic team data scraping
- Simple statistics display and tracking
- Local CSV file management
- Core dashboard functionality
