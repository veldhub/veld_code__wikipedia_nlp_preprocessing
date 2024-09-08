FROM debian:bullseye-20240513-slim
RUN apt update
RUN apt install -y python3=3.9.2-3
RUN apt install -y python3-pip=20.3.4-4+deb11u1
RUN apt install -y wget=1.21-1+deb11u1
RUN pip install wikiextractor==3.0.6
RUN pip install ipdb==0.13.13
RUN pip install PyYAML==6.0.2 
RUN ln -s /usr/bin/python3 /usr/bin/python 

