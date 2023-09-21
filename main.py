import streamlit as st
from youtube_engine import get_channel_info
from mongodb_engine import load
from sql_engine import *
#App Title
st.title(":red[Youtube] Channel Analytics ")

st.write("Enter the Youtube Channel id")
# inputting url

with st.form(key='my_form'):
      
	text_input = st.text_input(label="Paste the channel Id here")
	submit_button = st.form_submit_button(label=':blue[Submit]')

#extraction message
if submit_button:
    st.write('Fetching Channel information from Datalake')
    st.success('Successfully updated', icon="✅")


# Create a database connection
conn = sqlite3.connect('my_database.db')
cursor = conn.cursor()

# Create a table to store names if it doesn't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS names (name TEXT)''')
conn.commit()

# Streamlit app
st.title("Name Checker and Adder")

# Input for the name
name = st.text_input("Enter a name:")

# Check if the name exists in the database
if name:
    cursor.execute("SELECT name FROM names WHERE name=?", (name,))
    result = cursor.fetchone()
    
    if result:
        st.write(f"The name '{name}' is already in the database.")
    else:
        st.write(f"The name '{name}' is not in the database.")
        if st.button("Add to Database"):
            cursor.execute("INSERT INTO names (name) VALUES (?)", (name,))
            conn.commit()
            st.success(f"'{name}' has been added to the database!")

# Additional buttons and draggable element
if st.button("Click Me!"):
    st.write("You clicked the button!")

if st.checkbox("Show Gimmick"):
    st.write("This is a gimmick!")

# Draggable element
st.write("Drag this element:")
draggable = st.slider("Draggable Slider", 0, 100, 50)
