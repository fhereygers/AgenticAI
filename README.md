# Agentic AI
This repository contains all documentation and artifacts to setup the new genAI Hands on Workshop.

The exercise guide (Labs) can be found in this readme markdown file.

## Project Structure

The project is organized with the following folder structure:

```
.
├── code/              # Backend scripts, and notebooks needed to create project artifacts
├── data/              # data that needs to be loaded into the warehouse
├── images/            # A collection of images referenced in project docs
├── tools /            # python tools that need be loaded into the tool template
```

## Use Case Intelli Banking

<br/>
<img src="images/usecase.png" width="90%">
 <br/>

**Use Case Scenario**: Intelli Banking delivers hyper-personalized banking experiences using AI Agents that analyze customer behavior, predict financial needs, review competitive offerings, and recommend the right products — all in real-time with Cloudera AI.

**Objective**: Maximize customer lifetime value and drive product adoption by leveraging AI agents that proactively identify financial needs, deliver hyper-personalized recommendations, and adapt instantly to changing market dynamics.

**Business Impact**: 
- Enhanced Customer Engagement: AI-driven personalization boosts customer satisfaction and loyalty through tailored financial solutions.
- Increased Revenue Opportunities: Real-time insights enable timely cross-sell and upsell, maximizing product adoption and lifetime value.
- Competitive Agility: Continuous monitoring of market offers ensures your bank stays ahead with relevant, attractive products.
- Operational Efficiency & Compliance: Automated analysis and decision-making reduce manual workload while ensuring transparent, auditable, and compliant processes.

By following the lab exercises, you will understand how with cloudera AI you can author and deploy your own agentic workflows:

- Create Workflow
- by creating Agents
- adding tools for accessing data
- deploy your workflow
- revisit the workflow and replace part of the tools with an mcp server


## Lab 1: Log in and Settings configuration

#### Login into the CDP tenant

Welcome to the virtual hands on Lab of October 9th. 2025. You all have been assigned a unique user name.

Search for your first name and the initial of your last name and memorize the assigned user name.

<br/>
<img src="images/partlistsep.png" width="60%">
<br/>
<br/>

When you enter the url in your browser, that was shared in the chat, you get following login page, where you now enter
your given user name and password

![cdptenantmarketing](images/cdptenantmarketing.png)

In case of success you should get to this home page of the CDP tenant:
![cdphomepage](images/cdphomepage.png)

- Go to the Workshop CDP Tenant
- Navigate to the CLoudera AI tile from the CDP Menu.
- Click into the Workbench by clicking the Workbench name.
- 
![workspacelist](images/workbenchlist.png)

A Workspace is a cluster that runs on a kubernetes service to provide teams of data scientists a platform to develop, test, train, and ultimately deploy machine learning models. It is designed to deploy a small number of infra resources and then autoscale compute resources as needed when end users implement more workloads and use cases.

- Click on *User Settings* in the left panel
- Go to Environment Variables tab and set your WORKLOAD_PASSWORD (this is the same as your login password for your User0xx ).

![password](images/password.png)

- Go to “Projects” on the left side panel > Select “Public Projects” from the dropdown.
- Click on the publicly available project named **AgenticAI**.

<br/>
<img src="images/pubproj.png" width="90%">
 <br/>

- Click on AI Studios > Agent Studio in the left menu
- then click **get started** to get to the home screen of Agent Studio

<br/>
<img src="images/agenticaihome.png" width="90%">
 <br/>


## Lab 2: Start creating workflow with agents

Take a moment to get familiar with the Agent Studio application.

In the top right corner you see four tabs: Agentic Workflow, Tools Catalog , LLMs and Feedback.

By default you are in the Agentic Workflow tab, and below you find an overview of deployed workflows (if any), Draft workflows (if any) and workflow templates.

Workflow templates allow you to create a new workflow based upon a template. This is a sort of quick start or samples to give ideas on how to typically build workflows.

<br/>
<img src="images/agentstudiohome.png" width="90%">
 <br/>

However we are going to build a complete workflow from scratch (sorry about this :-))

To do so, in the Agentic workflow tab:
- click on the button **Create**
- You are then asked to give a workflow name, so enter: **" 'your user name' Intelli Banking"** and then click **Create Workflow** at the right bottom of the screen
- Set Conversational: ON
- Set Manager Agent: ON

<br/>
<img src="images/workflowstart2.png" width="90%">
 
 <br/>


 - Click on the  **Create or Edits Agents** button, in order to create following 4 Agents, by filling out the fields accordingly for each single agent and by clicking **create Agent** at the lower right of the screen (use the openai option for LLM): 


| Name | Customer Analyst |
| ----------- | ----------- |
| Role | Customer Segmentation and Profiling Specialist |
| Backstory | Skilled in profiling and segmentation using customer data for personalized banking strategies. |
| Goal | Identify customer personas based on income, balances, loans, and demographic data from customer_profiles table. |


| Name | Transactions Analyst |
| ----------- | ----------- |
| Role | Behavioral and Spend Pattern Analyst |
| Backstory | Specializes in behavioral analytics to support targeting for marketing and engagement. |
| Goal | Detect customer behavioral patterns using transaction_data to surface high-potential leads or risk indicators. |

| Name | Product Recommender |
| ----------- | ----------- |
| Role | Personalized Product Recommendation Engine |
| Backstory | Product expert helping align offerings with usage trends and customer potential. |
| Goal | Match bank products (credit cards, loans) to customer profiles and behaviors using product_offers. |

| Name | Competitive Insights |
| ----------- | ----------- |
| Role | External Market Intelligence Agent |
| Backstory | Competitive intelligence agent scraping the latest banking promotions. |
| Goal | Scrape live web content to identify loans, credit offers offers, compare with internal product catalog, and propose competitive positioning. |


When you finished entering the above fields and saved the created agents, your screen should look like this:

 <br/>
<img src="images/createagents.png" width="90%">
 
 <br/>

Take a moment to select each agent tile on the left hand side, and review the agent settings on the right hand. So we define an agents behaviour by means of natural language that are leveraged as prompts when calling the LLM.

- When you finished reviewing click **close** at the bottom right, to get to the workflow page page again.

At the top of the screen you see that we are in the **Add Agents** step and you see also the next steps. 
On the right you see a graph that links the agents with a default manager.

In the next lab we are going to add tools to the 4 agents, so they can do actions.

 <br/>
<img src="images/agentsresult.png" width="90%">
 
 <br/>


## Lab 3: Add Tools to all four agents

- Click on the **Create or Edit Agents** button again.
- For each of the agents, created in lab 2, select each agent one by one and add the relevant tool by clicking the **Create or Edit Tools** button

For the Agents **Customer Analyst, Transactions Analyst and Product Recommender**:

- select the tool **hol db tool** out of the tool template 
- click **Create tool from Template**.
- click **Save tool** and then **close**

For the Agent **Competitive Insights**:

- select the tool **hol web scraper** out of the tool template 
- click **Create tool from Template**.
- click **Save tool** and then **close**



**Tip: you can limit the selection of tool templates by entering **hol** in the search template mask**

 <br/>
<img src="images/selecttool.png" width="80%">
 
 <br/>

When finished, you can once again click through the agents again and verify all agents got the right tool assigned.

<br/>
<img src="images/agentwithtool.png" width="80%">
 
 <br/>

- Click **close** to get to the "Edit Workflow" page.

Now the graph on the right hand side should contain also the right tools for each agent. 

 

We are finished with the **Add Agents** step. 

<br/>
<img src="images/workflowwithtools.png" width="90%">
<br/>

## Lab 4 configure Workflow and test draft workflow

- Click on the **Save & Next** button to get in the next step "Add tasks". As it is a converstional workflow no additional setting is required.
- Click again on the **Save & Next** button to get to the "configure" step.
- Enter your the required parameters for all tools, the below image should give you hints on the values. Otherwise check the chat for information.
- Click the **Save & Next** button to get to the "Test" step.
  

 <br/>
<img src="images/configinput2.png" width="70%">
 <br/>

 Test the workflow with following test questions:
 - How many customers do we have in Dubai?
 - Who are the top spenders in the last 30 days?
 - Who are the top 3 credit card customers based on their total spending?

Take some time and browse through the playback / logs and monitoring to understand how the agents cooperate together to get to the right answers.

<br/>
<img src="images/playback.png" width="90%">
 <br/>

We use Phoenix to instrument our workflows for observability. Phoenix is an open-source AI observability platform designed for experimentation, evaluation, and troubleshooting.
Click on Monitoring to leverage Phoenix e.g. for troubleshooting




## Lab 5  Deploy Workflow 

Now Deploy your worklflow BUT also save your work as workflow template!

Wait for the deployed workflow to be up and running.
You can find your deployed workflow by:
- Going to the Home Page of Agent Studios.
- Select your Workflow from Deployed Workflows.
- Click on Open Application UI.

and then ask following questions:
- which are the top 3 spending categories?
- What are the credit card offers from barclays bank www.barclays.co.uk ?
- Compare our credit card offers with barclays bank www.barclays.co.uk ?
- Which customers are eligible for these offers and share their details?



## Lab 6 leverage MCP server instead of hol db tool
 
- Create a new workflow, the template you saved in lab 5.
- Replace for the relevant 3 agents the **hol db tool** with the mcp server that is available in your environment.
- Test the workflow, by entering the previous questions.












.

