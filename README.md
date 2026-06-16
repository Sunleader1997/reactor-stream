# Reactor Stream

基于 Project Reactor 的高级响应式数据流管道库。

## 简介

reactor-stream 提供了更简洁的 API 来构建响应式数据流管道，内置线程池管理和背压处理机制。它封装了 Project Reactor 的复杂性，让开发者能够更专注于业务逻辑。

## 特性

- **双模式数据源**：支持推送（Listen）和拉取（Pull）两种数据摄取方式
- **自动背压控制**：内置并发限制和缓冲机制
- **资源管理**：自动管理线程池和调度器
- **简洁 API**：通过 `createConsumer` 一行代码创建数据处理管道

## 快速开始

### 添加依赖

Maven:
```xml
<dependency>
    <groupId>io.github.sunleader1997</groupId>
    <artifactId>reactor-stream</artifactId>
    <version>0.0.1</version>
</dependency>
```

### 拉取模式示例

```java
// 创建数据源
DataSourceByPull<String> dataSource = new DataSourceByPull<>("myPool") {
    @Override
    protected Mono<String> fetchData() {
        // 从外部系统获取数据
        return Mono.just("data");
    }
};

// 创建消费者管道
dataSource.createConsumer(mono -> mono
    .map(data -> process(data))
    .doOnNext(result -> System.out.println(result))
);

// 关闭资源
dataSource.close();
```

### 推送模式示例

```java
// 创建数据源
DataSourceByListen<String> dataSource = new DataSourceByListen<>();

// 创建消费者管道
dataSource.createConsumer(mono -> mono
    .map(data -> process(data))
    .doOnNext(result -> System.out.println(result))
);

// 推送数据
dataSource.tryEmit("data1");
dataSource.tryEmit("data2");

// 关闭资源
dataSource.close();
```

## 架构

```
AbstractNode<T> (AutoCloseable)
  └── DataSourceAbsNode<T>
        ├── DataSourceByListen<T>   # 推送模式
        └── DataSourceByPull<T>     # 拉取模式
```

### 核心组件

- **AbstractNode**：所有节点的基类，实现 AutoCloseable
- **DataSourceAbsNode**：数据源抽象类，管理生产者和消费者生命周期
- **DataSourceByPull**：拉取模式实现，内部循环调用 fetchData()
- **DataSourceByListen**：推送模式实现，通过 tryEmit() 接收数据
- **WorkSpaceEnv**：线程池和调度器管理

## 构建

```bash
# 编译
./mvnw compile

# 运行示例
./mvnw test

# 打包 JAR
./mvnw package
```

## 系统要求

- Java 17+
- Maven 3.6+

## 许可证

本项目采用 MIT 许可证。
