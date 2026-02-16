FROM rust:latest
WORKDIR /usr/local/src/app
COPY . .
RUN cargo install --path=.  
CMD [ "convert-invert" ]
