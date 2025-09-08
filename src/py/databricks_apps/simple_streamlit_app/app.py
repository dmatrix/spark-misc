import streamlit as st

def main():
    st.set_page_config(
        page_title="Simple Databricks App",
        page_icon="🚀",
        layout="centered"
    )
    
    st.title("🚀 Simple Databricks Streamlit App")
    st.markdown("A simple app with a button that works!")
    
    # Create the button
    if st.button("Press Me", type="primary"):
        st.success("Yeah, It works!")
    
    # Alternative: Always show the text field, but only populate it when button is pressed
    if "button_pressed" not in st.session_state:
        st.session_state.button_pressed = False
    
    # Button that sets session state
    if st.button("Press Me (Persistent)", key="persistent_button"):
        st.session_state.button_pressed = True
    
    # Display text based on session state
    if st.session_state.button_pressed:
        st.text_input("Output:", value="Yeah, It works!", disabled=True)

if __name__ == "__main__":
    main()
