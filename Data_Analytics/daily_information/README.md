### Automating Daily Analytics Report for the Application

The goal was to streamline the process of disseminating key metrics and insights to stakeholders every morning via Telegram.

1. **Creating a Telegram Bot:**
    - Utilized @BotFather to create a Telegram bot, obtaining the necessary token for integration. Leveraged the provided link or the bot.getUpdates() method to retrieve the chat_id.

2. **Script Development for Report Compilation:**
    - Developed a script to compile the news feed report, featuring two primary sections:
        - Textual summary containing information about the values of key metrics for the previous day.
        - Graphical representation displaying metric values for the past 7 days.
    - Key metrics included DAU, Views, Likes, and CTR.

3. **Airflow Integration for Automation:**
    - Integrated Airflow to automate the process of generating and sending the report. Organized the code for report generation within the GitLab repository, adhering to the specified directory structure.

4. **Application Performance Report:**
    - Curated a comprehensive report encompassing the overall performance of the application as a unified entity. Conducted a detailed analysis of various aspects of the application.
    - Identified essential metrics relevant to the entire application or segmented metrics for distinct components such as the news feed and messaging feature.
    - Incorporated graphs and supplementary files to provide visual representation and facilitate better understanding of the data.
    - Compiled all aspects of the application analysis into a comprehensive PDF document. Ensured that the report was structured logically and contained insights derived from the data.

Through this project, I honed my skills in data automation, report generation, and Airflow utilization, gaining practical insights into orchestrating data workflows and delivering actionable insights to stakeholders effectively.  
This experience empowered me to contribute meaningfully to the automation and optimization of data processes within our application ecosystem, facilitating informed decision-making and continuous improvement.