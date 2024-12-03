### **Predicting Car Crash Severity: Machine Learning Analysis of Chicago Police Department Data**

🚗🔍 **Welcome to the Car Crash Severity Prediction Project!**  
This repository showcases a data science project aimed at predicting the severity of car crashes using historical data from the Chicago Police Department. By leveraging machine learning in **R**, this project provides actionable insights to improve traffic safety and reduce the impact of severe crashes.

---

### **Table of Contents**
- [Overview](#overview)
- [Dataset](#dataset)
- [Technologies Used](#technologies-used)
- [Project Workflow](#project-workflow)
- [Key Features](#key-features)
- [Results](#results)
- [Usage](#usage)
- [Contributing](#contributing)
- [Contact](#contact)

---

### **Overview**
The project analyzes over **500,000 traffic incidents** to predict crash severity based on features such as weather, road conditions, and time of day. This analysis aims to assist policymakers and urban planners in making data-driven decisions to enhance public safety.

---

### **Dataset**
- **Source**: Chicago Police Department Traffic Crash Dataset  
- **Key Features**:
  - Crash type (property damage, injury, or fatality)
  - Weather and road conditions
  - Driver behavior
  - Temporal data (time of day, day of week)

---

### **Technologies Used**
- **Programming Language**: R  
- **Tool**: RStudio  
- **Libraries**:  
  - `dplyr`, `tidyr` for data manipulation  
  - `ggplot2` for visualization  
  - `caret`, `xgboost` for machine learning  
- **Techniques**:
  - Data Cleaning and Preprocessing
  - Feature Engineering
  - Handling Class Imbalance (SMOTE)
  - Machine Learning Model Development

---

### **Project Workflow**
1. **Data Preprocessing**:  
   Cleaned, transformed, and prepared data for analysis using `dplyr` and `tidyr`.
2. **Exploratory Data Analysis (EDA)**:  
   Identified trends and correlations, visualized key patterns using `ggplot2`.
3. **Model Training**:  
   Applied and fine-tuned machine learning models (e.g., Random Forest, XGBoost) using `caret` and `xgboost`.
4. **Evaluation**:  
   Assessed models using metrics such as accuracy, precision, recall, and F1-score.
5. **Results Interpretation**:  
   Derived actionable insights to guide traffic safety interventions.

---

### **Key Features**
- **Predictive Modeling**: Classifies crashes into severity levels with high accuracy.  
- **Insights for Policy**: Identifies high-risk scenarios for targeted interventions.  
- **Interactive Visualizations**: Illustrates trends in crash data for easy understanding.  

---

### **Results**
- **Model Performance**:  
  Achieved a **20% improvement** in predictive accuracy compared to baseline models.  
- **Key Insights**:  
  - Weather and road conditions significantly impact crash severity.  
  - Specific time periods and locations are higher-risk zones.  
- **Impact**:  
  Recommended safety measures, including enhanced lighting and road maintenance during adverse weather.

---

### **Usage**
1. Clone the repository:  
   ```bash
   git clone https://github.com/gopi-0707/PREDICTING-CAR-CRASH-SEVERITY.git
   cd PREDICTING-CAR-CRASH-SEVERITY
   ```
2. Open the RStudio project file:  
   ```bash
   DPA_PROJECT.R
   ```
3. Install required libraries:  
   ```R
   install.packages(c("dplyr", "tidyr", "ggplot2", "caret", "xgboost"))
   ```
4. Execute the analysis:  
   Open `DPA_PROJECT.R` in RStudio and run the script.

---

### **Contributing**
Contributions are welcome! If you have ideas or enhancements, feel free to submit a pull request or open an issue.

---

### **Contact**
For any questions or collaborations, reach out:  
**Name**: Bala Gopinath Kuchibatla  
📧 Email: [bkuchibatla@hawk.iit.edu](mailto:bkuchibatla@hawk.iit.edu)  
🌐 LinkedIn: [https://www.linkedin.com/in/gopi-k-73ab77179/](https://www.linkedin.com/in/gopi-k-73ab77179/)

---

### 🌟 **If you find this project interesting, give it a ⭐ and share it!**  
