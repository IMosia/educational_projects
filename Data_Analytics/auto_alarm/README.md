### Automating Anomaly Detection for Application Metrics: My Journey

I built a system to monitor key metrics of our application at 15-minute intervals.  
The goal was to trigger alerts when anomalies occurred, facilitating prompt investigation and response.

**Approach:**

1. **Understanding Metric Behavior:**
   - Analyzed metric behavior to identify patterns and fluctuations.
   - Explored deviations in user categories and geographical usage alongside metric analysis.

2. **Selecting Anomaly Detection Method:**
   - Chose a simple method to compare current values to historical ones.
   - Evaluated deviations to determine anomalies exceeding predefined thresholds.

3. **Alerting System Design:**
   - Designed an alerting system to notify stakeholders via Telegram.
   - Crafted alert messages to include metric details.

4. **Automating with Airflow:**
   - Integrated Airflow for automation, creating a DAG file for scheduled execution.

This experience provided practical insights into anomaly detection and workflow automation, enabling proactive maintenance of application stability and performance.