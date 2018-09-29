FROM scratch
WORKDIR /
ADD orion-trace-aggregator /orion-trace-aggregator
ADD default.toml /default.toml
CMD ['./orion-trace-aggregator']