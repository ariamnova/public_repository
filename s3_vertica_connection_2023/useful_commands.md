# Final Project

# Final project commands


1. **Create directory**:
- for dags: *docker exec -it <container ID> mkdir /lessons/dags --parents*
- for sql code: *docker exec -it <container ID> mkdir /lessons/dags/sql --parents*

2. **Install library** in docker to use vertica:
- *docker exec -it 5c8103f3ded2 pip install vertica-python*

3. **Copy files** to docker:
- *docker cp <dir to file>/<file name> <container ID>:/<dir in docker>/<file name>*

4. **Metabase installation**:
- copy driver to docker: *docker cp /dir_to_driver_folder/vertica-jdbc-11.0.2-0.jar <container ID>:/opt/metabase/plugins*
- kill java process: *docker exec -it <container ID> sh -c "pkill java"*
- check that it was killed: *docker exec <container ID> ps -ef | grep java*
- check Metabase state: *docker logs -f metabase*
- check that driver was successfully installed:
    * *docker exec -it <CONTAINER_ID> bash*
    * *cd /opt* ...etc till the plugins in metabase dir