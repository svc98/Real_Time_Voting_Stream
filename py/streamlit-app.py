import streamlit as st
import time
import psycopg2
import matplotlib.pyplot as plt
import pandas as pd
import simplejson as json
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh

# Variables Setup
votes_topic = "aggregated_votes_per_candidate"
location_topic = "aggregated_turnout_by_location"


@st.cache_data()
def fetch_voting_stats():
    # Connect to Postgres database
    conn = psycopg2.connect("host=localhost dbname=voting user=postgres password=postgres")
    cursor = conn.cursor()

    # Fetch total number of candidates
    cursor.execute("""
        SELECT count(*) as candidates_count from candidates
    """)
    candidates_count = cursor.fetchone()[0]
    print(candidates_count)

    # Fetch total number of voters
    cursor.execute("""
        SELECT count(*) as voters_count from voters
    """)
    voters_count = cursor.fetchone()[0]
    print(voters_count)
    return candidates_count, voters_count

def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:29092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def fetch_data_from_kafka(consumer):
    messages = consumer.poll(timeout_ms=1000)
    data = []
    for msg in messages.values():
        for sub_msg in msg:
            data.append(sub_msg.value)                                                                                  # Mess here
    return data

@st.cache_data
def process_data(data):
    results = pd.DataFrame(data)
    return results

def plot_colored_bar_chart(results):
    data_labels = results['candidate_name']
    plt.bar(data_labels, results['total_votes'])
    plt.title("Vote Counts per Candidate")
    plt.xlabel("Candidate")
    plt.ylabel("Total Votes")
    plt.xticks(rotation=90)
    return plt

def plot_donut_chart(results):
    labels = results['candidate_name']
    size = results['total_votes']

    fig, ax = plt.subplots()
    ax.pie(size, labels=labels, autopct='%1.1f%%', startangle=140)
    ax.axis('equal')
    plt.title("Vote Distribution")
    return fig


# Online Custom Function to split a dataframe into chunks for pagination
@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i: i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df

def paginate_table(table_data):
    top_menu = st.columns(3)
    with top_menu[0]:
        sort = st.radio("Sort Data", options=["Yes", "No"], horizontal=True, index=1, key="sort_radio")
    if sort == "Yes":
        with top_menu[1]:
            sort_field = st.selectbox("Sort By", options=table_data.columns, key="sort_field")
        with top_menu[2]:
            sort_direction = st.radio(
                "Direction", options=["⬆️", "⬇️"], horizontal=True, key="sort_direction"
            )
        table_data = table_data.sort_values(
            by=sort_field, ascending=sort_direction == "⬆️", ignore_index=True
        )

    pagination = st.container()

    bottom_menu = st.columns((4, 1, 1))
    with bottom_menu[2]:
        batch_size = st.selectbox("Page Size", options=[10, 25, 50, 100], key="batch_size")
    with bottom_menu[1]:
        total_pages = (
            int(len(table_data) / batch_size) if int(len(table_data) / batch_size) > 0 else 1
        )
        current_page = st.number_input(
            "Page", min_value=1, max_value=total_pages, step=1, key="current_page"
        )
    with bottom_menu[0]:
        st.markdown(f"Page **{current_page}** of **{total_pages}** ")

    pages = split_frame(table_data, batch_size)
    pagination.dataframe(data=pages[current_page - 1], use_container_width=True)


def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 5)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()

def update_data():
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")


    # Fetch data from Kafka: Aggregated votes per candidate
    consumer = create_kafka_consumer(votes_topic)
    data = fetch_data_from_kafka(consumer)
    results = process_data(data)

    # Identify the leading candidate
    results = results.loc[results.groupby('candidate_id')['total_votes'].idxmax()]                                      # Kafka topic will contain old records as well, need current
    leading_candidate = results.loc[results['total_votes'].idxmax()]                                                    # Run again to get the highest value

    # Display the Leading Candidate
    st.markdown("""---""")
    st.header('Leading Candidate')
    col1, col2 = st.columns(2)
    with col1:
        st.image(leading_candidate['photo_url'], width=200)
    with col2:
        st.header(leading_candidate['candidate_name'])
        st.subheader(leading_candidate['party_affiliation'])
        st.subheader("Total Votes: {}".format(leading_candidate['total_votes']))

    # Display the Stats and Visualizations
    st.markdown("""---""")
    st.header('Voting Statistics')
    results = results[['candidate_id', 'candidate_name', 'party_affiliation', 'total_votes']]
    results = results.reset_index(drop=True)

    # Display Bar and Donut Charts
    col1, col2 = st.columns(2)
    with col1:
        bar_fig = plot_colored_bar_chart(results)
        st.pyplot(bar_fig)
    with col2:
        donut_fig = plot_donut_chart(results)
        st.pyplot(donut_fig)

    st.markdown("""---""")
    st.header('Raw Table Data')
    st.table(results)



    # Fetch data from Kafka: Turnout by location
    location_consumer = create_kafka_consumer(location_topic)
    location_data = fetch_data_from_kafka(location_consumer)
    location_results = pd.DataFrame(location_data)

    # Max location
    location_result = location_results.loc[location_results.groupby('state')['count'].idxmax()]
    location_result = location_result.reset_index(drop=True)

    # Display location-based voter information with pagination
    st.markdown("""---""")
    st.header("Location of Voters")
    paginate_table(location_result)

    # Update the last refresh time
    st.session_state['last_update'] = time.time()


# Web UI
st.title("Realtime Election Voting Dashboard")
sidebar()
update_data()