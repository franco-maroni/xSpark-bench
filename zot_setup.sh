#!/usr/bin/env bash
Z3_VERSION="4.7.1"
TMP_BUILD_DEPS="
           ca-certificates
           gcc
           g++
           make
           zlib1g-dev
           unzip
           curl
           "
# update and install needed libraries
sudo apt-get -q update && sudo apt-get install -y \
    $TMP_BUILD_DEPS \
    libgomp1 \
    sbcl \
    python-dev \
    python-pip \
    graphviz && \
# setup minisat
curl -L http://minisat.se/downloads/minisat-2.2.0.tar.gz -o /tmp/minisat-2.2.0.tar.gz \
    && cd /tmp \
    && tar -xvzf minisat-2.2.0.tar.gz \
    && export MROOT=/tmp/minisat \
    && cd minisat/core/ \
    && make  \
    && sudo cp minisat /usr/local/bin/minisat \
    && make clean && \
curl -L https://github.com/Z3Prover/z3/releases/download/z3-${Z3_VERSION}/z3-${Z3_VERSION}-x64-ubuntu-14.04.zip -o /tmp/z3.zip \
    && unzip /tmp/z3.zip -d /tmp \
    && sudo cp /tmp/z3-${Z3_VERSION}-x64-ubuntu-14.04/bin/z3 /usr/local/bin/z3 \
    && sudo chmod a+x /usr/local/bin/z3 && \
# setup zot
sudo git clone https://github.com/fm-polimi/zot /usr/local/zot/ \
    && sudo mkdir -p /usr/lib/sbcl/site-systems/ \
    && sudo ln -s /usr/local/zot/asdfs/*.asd /usr/lib/sbcl/site-systems/ \
    && sudo cp /usr/local/zot/bin/zot /usr/local/bin/ && \
# remove build deps
#apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false -o APT::AutoRemove::SuggestsImportant=false $TMP_BUILD_DEPS \
#    && rm -rf /tmp
sudo pip install --upgrade pip && \
sudo pip install virtualenv && \
cd ~ \
&& git clone --recurse-submodules -b features/spark/uppaal --single-branch https://github.com/dice-project/DICE-Verification  && \
#
cd /home/ubuntu/DICE-Verification/d-vert-server/d-vert-json2mc \
&& virtualenv venv \
&& . venv/bin/activate \
&& pip install . \
&& deactivate && \
#uppaal setup
sudo unzip /home/ubuntu/uppaal64-4.1.19.zip -d /usr/local \
&& printf "PATH=/usr/local/uppaal64-4.1.19/bin-Linux/:\$PATH" >> ~/.bash_profile

## add virtualenvwrapper scripts to .bash_profile
pip install --user virtualenvwrapper \
&& printf '\n%s\n%s\n%s' '# virtualenv' 'export WORKON_HOME=~/virtualenvs' \
   'source  /home/ubuntu/.local/bin/virtualenvwrapper.sh' >> ~/.bash_profile
. ~/.bash_profile
mkdir -p $WORKON_HOME
#mkvirtualenv d-vert -a /home/ubuntu/DICE-Verification/d-vert-server/d-vert-json2mc
#workon d-vert