# 🏖️ Holiday Request Manager

*Because even data engineers need vacation days (and someone needs to approve them)*

## 📖 Overview

Welcome to the Holiday Request Manager - a full-stack Databricks App that proves you can build enterprise-grade applications without losing your sanity or your hair! This app demonstrates the power of combining **Databricks Lakebase** (our transactional PostgreSQL database) with **Databricks Apps** (our serverless application runtime) to create a seamless holiday approval workflow.

Gone are the days of juggling separate databases, authentication systems, and deployment pipelines. With this setup, everything lives happily together in the Databricks ecosystem - like a well-organized vacation itinerary, but for your data stack.

## 🎯 What This App Does

This isn't just another CRUD app (though it does Create, Read, Update, and Delete with style). It's a complete holiday request management system that lets:

- **Employees** submit holiday requests (virtually - we're not *that* advanced yet)
- **Managers** review, approve, or decline requests with a single click
- **Everyone** see the status of requests in a beautiful, color-coded interface

Think of it as the Switzerland of holiday management - neutral, efficient, and everyone wants to go there.

## 🏗️ Architecture: The Dream Team

This app showcases the perfect marriage between:

### 🗄️ Lakebase (The Reliable Partner)
- **Fully managed PostgreSQL** - No more 3 AM database maintenance calls
- **Low-latency reads/writes** - Faster than your manager's "quick sync" meetings
- **Built-in security** - More secure than your vacation photos
- **Seamless lakehouse integration** - Because data silos are so 2020

### 🚀 Databricks Apps (The Smooth Operator)
- **Serverless runtime** - Scales like your vacation wishlist
- **Built-in authentication** - No more password sticky notes
- **Unity Catalog integration** - Governance that actually works
- **One-click deployment** - Easier than booking a flight (usually)

## 🚀 Quick Start Guide

### Prerequisites
- A Databricks workspace (obviously)
- A sense of humor (recommended)
- Coffee (essential for any data project)

### Step 1: Set Up Lakebase
1. Navigate to **Compute** → **OLTP Database** in your Databricks workspace
2. Create a new Lakebase instance (we called ours `lakebase-demo-instance`)
3. Grab a coffee while it provisions ☕

### Step 2: Deploy the App
1. Create a new Databricks App
2. Add the **Database** resource and select your Lakebase instance
3. Upload this code or clone from the repository
4. Deploy and watch the magic happen ✨

### Step 3: Initialize the Database
Run the SQL script in `create_tables_and_schema.sql` to:
- Create the `holidays` schema
- Set up the `holiday_requests` table
- Insert sample data for our amazing team
- Configure permissions (because security matters)

```sql
-- The script includes requests for our stellar team:
-- Andre, Carly, Daniel, Denny, Elise, Holly, Jenni, 
-- Jules, Lizzie, Nick, Oleksandra, Robert, and Torey
-- All requesting December 1-12, 2025 (great minds think alike!)
```

## 🎨 Features That'll Make You Smile

### 🔘 Radio Button Perfection
- **Single-selection radio buttons** - Because choosing multiple vacation requests would be chaos
- **Visual feedback** - Selected rows light up like your face when vacation is approved
- **Intuitive interface** - So simple, even your manager can use it

### 🎨 Status Color Coding
- 🟡 **Pending** - The suspense is killing us
- 🟢 **Approved** - Time to pack those bags!
- 🔴 **Declined** - Better luck next time (or find a new manager)

### 💬 Manager Comments
- Add context to decisions
- Provide feedback (constructive, we hope)
- Document the "why" behind approvals/rejections

## 🔧 Technical Deep Dive

### Database Connection Magic
```python
# OAuth token authentication - because passwords are passé
@event.listens_for(postgres_pool, "do_connect")
def provide_token(dialect, conn_rec, cargs, cparams):
    cparams["password"] = workspace_client.config.oauth_token().access_token
```

### Streamlit UI Components
- **Dynamic table rendering** with proper column layouts
- **Session state management** for seamless user experience
- **Real-time updates** with `st.rerun()` magic
- **Error handling** that actually helps users

### Security & Permissions
- **Client ID-based access control** - Each app gets its own identity
- **Fine-grained table permissions** - Only touch what you need
- **Unity Catalog integration** - Governance without the headaches

## 📁 Project Structure

```
holiday_request_app/
├── app.py                          # Main Streamlit application
├── app.yaml                        # App configuration
├── requirements.txt                # Python dependencies
├── pyproject.toml                  # Project metadata
├── create_tables_and_schema.sql    # Database setup script
└── README.md                       # This delightful document
```

## 🔗 Dependencies

The app uses a carefully curated selection of packages:
- `streamlit` - For the beautiful UI
- `pandas` - For data wrangling wizardry
- `databricks-sdk` - For seamless platform integration
- `sqlalchemy` - For elegant database operations
- `psycopg` - For PostgreSQL connectivity

## 🎭 Sample Data

Our test dataset includes holiday requests from our fantastic team, all wanting the same December vacation period. It's like they planned it... or maybe great minds just think alike! 🤔

Each request includes:
- **Employee name** - Real people, real vacation dreams
- **Date range** - December 1-12, 2025 (prime holiday season)
- **Status** - All pending (the suspense!)
- **Manager notes** - Currently empty (awaiting your wisdom)

## 🚀 Extending the App

This is just the beginning! Consider adding:
- **Email notifications** - Automated approval/rejection emails
- **Calendar integration** - Sync with corporate calendars
- **Reporting dashboard** - Analytics on vacation patterns
- **Mobile responsiveness** - Approve requests from the beach
- **Slack integration** - Because everything needs Slack these days

## 🎉 Why This Matters

This app demonstrates how [Databricks Lakebase and Apps](https://www.databricks.com/blog/how-use-lakebase-transactional-data-layer-databricks-apps) eliminate the traditional pain points of full-stack development:

- **No infrastructure juggling** - Everything runs on Databricks
- **No authentication headaches** - Built-in security that just works
- **No deployment nightmares** - One platform, one deployment
- **No data synchronization** - Lakebase integrates natively with your lakehouse

## 🤝 Contributing

Found a bug? Have an idea? Want to add more emoji? We welcome contributions! Just remember:
1. Keep it fun
2. Keep it functional
3. Keep it well-documented
4. Did we mention keep it fun?

## 📜 License

This project is licensed under the "Use It, Love It, Share It" license. Basically, have fun with it!

## 🙏 Acknowledgments

- The Databricks team for building an amazing platform
- Our test team members for "volunteering" their names
- Coffee, for making all of this possible
- Inspired by the [Databricks Blog](https://www.databricks.com/blog/how-use-lakebase-transactional-data-layer-databricks-apps) (and modified here)
- You, for reading this far (seriously, thanks!)

---

*Built with ❤️ and ☕ on the Databricks platform*

**Ready to revolutionize your holiday approval process?** Deploy this app and watch your team's vacation management transform from chaos to zen! 🧘‍♂️

---

*P.S. - If you're reading this README instead of taking a vacation, maybe it's time to submit a holiday request yourself! 😉*
