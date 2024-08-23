FROM ghcr.io/jac18281828/rust:latest

ARG PROJECT=rusthello
WORKDIR /workspaces/${PROJECT}
COPY --chown=rust:rust . .
ENV USER=rust
USER rust

ENV PATH=/home/rust/.cargo/bin:$PATH
# source $HOME/.cargo/env
RUN rustc --version

#RUN yamlfmt -lint .github/workflows/*.yml

#RUN cargo fmt --check
#RUN cargo clippy --all-features --no-deps
#RUN cargo test
#CMD cargo run
