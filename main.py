import streamlit as st

#App Title
st.title(":red[Youtube] Channel Analytics ")

st.write("Enter the Youtube Channel id")
# inputting url
with st.form(key='my_form'):
	text_input = st.text_input(label="Paste the channel url here")
	submit_button = st.form_submit_button(label=':blue[Submit]')

#extraction message
if submit_button:
    st.write('Fetching Channel information to upload in Datalake')
    st.success('Successfully updated', icon="✅")
#
