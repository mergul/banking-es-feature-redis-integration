# CMake generated Testfile for 
# Source directory: /home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests
# Build directory: /home/mesut/RustroverProjects/banking-es-kafka-redis/banking-es-feature-redis-integration/build/tests
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(RdKafkaTestInParallel "/home/mesut/RustroverProjects/banking-es-kafka-redis/banking-es-feature-redis-integration/build/tests/test-runner" "-p5")
set_tests_properties(RdKafkaTestInParallel PROPERTIES  _BACKTRACE_TRIPLES "/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;160;add_test;/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;0;")
add_test(RdKafkaTestSequentially "/home/mesut/RustroverProjects/banking-es-kafka-redis/banking-es-feature-redis-integration/build/tests/test-runner" "-p1")
set_tests_properties(RdKafkaTestSequentially PROPERTIES  _BACKTRACE_TRIPLES "/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;161;add_test;/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;0;")
add_test(RdKafkaTestBrokerLess "/home/mesut/RustroverProjects/banking-es-kafka-redis/banking-es-feature-redis-integration/build/tests/test-runner" "-p5" "-l")
set_tests_properties(RdKafkaTestBrokerLess PROPERTIES  _BACKTRACE_TRIPLES "/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;162;add_test;/home/mesut/RustroverProjects/banking-es-feature-redis-integration/librdkafka/tests/CMakeLists.txt;0;")
subdirs("interceptor_test")
