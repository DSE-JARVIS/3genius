# How to use ?

```from ibmaemagic import AnalyticEngineClient

#create client
ae = AnalyticEngineClient(host="dummy-host")

#create volume and upload file
ae.create_volume("VOLUME-NAME", create_arguments=create_arguments)
ae.start_volume("VOLUME-NAME")
ae.add_file_to_volume(...)

#create Analytics Engine Instance
ae.create_instance("AE_INSTANCE_NAME", create_arguments=create_arguments_instance)
ae.submit_word_count_job("AE_INSTANCE_NAME")

#download job logs
ae.download_logs("AE_INSTANCE_NAME", "VOLUME-NAME", "JOB-ID")

#delete Volume and Analytics instance
ae.delete_volume("VOLUME-NAME")
ae.delete_instance("AE_INSTANCE_NAME")

```

# 4genius
- Ravi
- Dheeraj
- Sepi
- Kai

# Run Test
* `pip install -e .`
* `pytest ./tests --cov=src`
* move to project root folder and 'pytest'
* more detial: https://docs.pytest.org/en/stable/goodpractices.html
