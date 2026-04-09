 def show_sql_chat_dialog():
    for message in st.session_state.sql_chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    user_message = st.chat_input("Ask about the generated SQL...")
    if user_message:
        st.session_state.sql_chat_messages.append({"role": "user", "content": user_message})

        with st.chat_message("user"):
            st.markdown(user_message)

        with st.chat_message("assistant"):
            try:
                chat_prompt = _build_sql_chat_prompt(
                    user_message,
                    st.session_state.get("staging_sql", ""),
                    st.session_state.get("transform_sql", ""),
                    st.session_state.get("business_sql", ""),
                )
                assistant_reply = generate(chat_prompt, provider=llm_provider)
                st.markdown(assistant_reply)
                st.session_state.sql_chat_messages.append({"role": "assistant", "content": assistant_reply})
            except Exception as e:
                error_message = f"Chat error: {str(e)}"
                st.error(error_message)
                st.session_state.sql_chat_messages.append({"role": "assistant", "content": error_message})
                
    if st.button("Close Chat", key="close_sql_chat_btn"):
    st.session_state.show_sql_chat = False
    st.rerun()


def _build_sql_chat_prompt(user_message, staging_sql, transform_sql, business_sql):
    """Build chatbot prompt with generated SQL context."""
    return f"""You are an SQL assistant helping users understand generated Snowflake ETL SQL.

Use the SQL context below to answer the user question clearly and accurately.

STAGING SQL:
{staging_sql}

TRANSFORM SQL:
{transform_sql}

BUSINESS SQL:
{business_sql}

USER QUESTION:
{user_message}

Answer directly and concisely based on the SQL above.
"""
