/*
 *    Copyright 2009-2021 the original author or authors.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.apache.ibatis.builder.xml;

import java.io.InputStream;
import java.io.Reader;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.ibatis.builder.BaseBuilder;
import org.apache.ibatis.builder.BuilderException;
import org.apache.ibatis.datasource.DataSourceFactory;
import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.executor.loader.ProxyFactory;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.io.VFS;
import org.apache.ibatis.logging.Log;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.parsing.XNode;
import org.apache.ibatis.parsing.XPathParser;
import org.apache.ibatis.plugin.Interceptor;
import org.apache.ibatis.reflection.DefaultReflectorFactory;
import org.apache.ibatis.reflection.MetaClass;
import org.apache.ibatis.reflection.ReflectorFactory;
import org.apache.ibatis.reflection.factory.ObjectFactory;
import org.apache.ibatis.reflection.wrapper.ObjectWrapperFactory;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.AutoMappingUnknownColumnBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.LocalCacheScope;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.type.JdbcType;

/**
 * @author Clinton Begin
 * @author Kazuki Shimizu
 */
public class XMLConfigBuilder extends BaseBuilder {

  private boolean parsed;
  private final XPathParser parser;
  private String environment;
  private final ReflectorFactory localReflectorFactory = new DefaultReflectorFactory();

  public XMLConfigBuilder(Reader reader) {
    this(reader, null, null);
  }

  public XMLConfigBuilder(Reader reader, String environment) {
    this(reader, environment, null);
  }

  public XMLConfigBuilder(Reader reader, String environment, Properties props) {
    this(new XPathParser(reader, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  public XMLConfigBuilder(InputStream inputStream) {
    this(inputStream, null, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment) {
    this(inputStream, environment, null);
  }

  public XMLConfigBuilder(InputStream inputStream, String environment, Properties props) {
    this(new XPathParser(inputStream, true, props, new XMLMapperEntityResolver()), environment, props);
  }

  private XMLConfigBuilder(XPathParser parser, String environment, Properties props) {
    super(new Configuration());
    ErrorContext.instance().resource("SQL Mapper Configuration");
    this.configuration.setVariables(props);
    this.parsed = false;
    this.environment = environment;
    this.parser = parser;
  }

  public Configuration parse() {
    if (parsed) {
      throw new BuilderException("Each XMLConfigBuilder can only be used once.");
    }
    parsed = true;
    parseConfiguration(parser.evalNode("/configuration"));
    return configuration;
  }

  private void parseConfiguration(XNode root) {
    try {
      // issue #117 read properties first
      propertiesElement(root.evalNode("properties"));
      Properties settings = settingsAsProperties(root.evalNode("settings"));
      loadCustomVfs(settings);
      loadCustomLogImpl(settings);
      typeAliasesElement(root.evalNode("typeAliases"));
      pluginElement(root.evalNode("plugins"));
      objectFactoryElement(root.evalNode("objectFactory"));
      objectWrapperFactoryElement(root.evalNode("objectWrapperFactory"));
      reflectorFactoryElement(root.evalNode("reflectorFactory"));
      settingsElement(settings);
      // read it after objectFactory and objectWrapperFactory issue #631
      environmentsElement(root.evalNode("environments"));
      databaseIdProviderElement(root.evalNode("databaseIdProvider"));
      typeHandlerElement(root.evalNode("typeHandlers"));
      mapperElement(root.evalNode("mappers"));
    } catch (Exception e) {
      throw new BuilderException("Error parsing SQL Mapper Configuration. Cause: " + e, e);
    }
  }

  private Properties settingsAsProperties(XNode context) {
    if (context == null) {
      return new Properties();
    }
    Properties props = context.getChildrenAsProperties();
    // Check that all settings are known to the configuration class
    MetaClass metaConfig = MetaClass.forClass(Configuration.class, localReflectorFactory);
    for (Object key : props.keySet()) {
      if (!metaConfig.hasSetter(String.valueOf(key))) {
        throw new BuilderException("The setting " + key + " is not known.  Make sure you spelled it correctly (case sensitive).");
      }
    }
    return props;
  }

  private void loadCustomVfs(Properties props) throws ClassNotFoundException {
    String value = props.getProperty("vfsImpl");
    if (value != null) {
      String[] clazzes = value.split(",");
      for (String clazz : clazzes) {
        if (!clazz.isEmpty()) {
          @SuppressWarnings("unchecked")
          Class<? extends VFS> vfsImpl = (Class<? extends VFS>)Resources.classForName(clazz);
          configuration.setVfsImpl(vfsImpl);
        }
      }
    }
  }

  private void loadCustomLogImpl(Properties props) {
    Class<? extends Log> logImpl = resolveClass(props.getProperty("logImpl"));
    configuration.setLogImpl(logImpl);
  }

  /**
   * 类型别名
   * <typeAliases>
   *   <typeAlias alias="Author" type="domain.blog.Author"/>
   *   <typeAlias alias="Blog" type="domain.blog.Blog"/>
   *   <typeAlias alias="Comment" type="domain.blog.Comment"/>
   *   <typeAlias alias="Post" type="domain.blog.Post"/>
   *   <typeAlias alias="Section" type="domain.blog.Section"/>
   *   <typeAlias alias="Tag" type="domain.blog.Tag"/>
   * </typeAliases>
   *   or
   * <typeAliases>
   *   <package name="domain.blog"/>
   * </typeAliases>
   */
  private void typeAliasesElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        // 如果是package
        if ("package".equals(child.getName())) {
          String typeAliasPackage = child.getStringAttribute("name");
          //（一）调用TypeAliasRegistry.registerAliases，去包下找所有类,然后注册别名(有@Alias注解则用，没有则取类的simpleName)
          configuration.getTypeAliasRegistry().registerAliases(typeAliasPackage);
        } else {
          // 如果是typeAlias
          String alias = child.getStringAttribute("alias");
          String type = child.getStringAttribute("type");
          try {
            Class<?> clazz = Resources.classForName(type);
            // 根据Class名字来注册类型别名
            //（二）调用TypeAliasRegistry.registerAlias
            if (alias == null) {
              // alias可以省略
              typeAliasRegistry.registerAlias(clazz);
            } else {
              typeAliasRegistry.registerAlias(alias, clazz);
            }
          } catch (ClassNotFoundException e) {
            throw new BuilderException("Error registering typeAlias for '" + alias + "'. Cause: " + e, e);
          }
        }
      }
    }
  }
  /**
   * 插件
   * MyBatis 允许你在某一点拦截已映射语句执行的调用。默认情况下,MyBatis 允许使用插件来拦截方法调用
   * <plugins>
   *   <plugin interceptor="org.mybatis.example.ExamplePlugin">
   *     <property name="someProperty" value="100"/>
   *   </plugin>
   * </plugins>
   */
  private void pluginElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        String interceptor = child.getStringAttribute("interceptor");
        Properties properties = child.getChildrenAsProperties();
        Interceptor interceptorInstance = (Interceptor) resolveClass(interceptor).getDeclaredConstructor().newInstance();
        interceptorInstance.setProperties(properties);
        // 调用InterceptorChain.addInterceptor
        configuration.addInterceptor(interceptorInstance);
      }
    }
  }

  /**
   * 对象工厂,可以自定义对象创建的方式,比如用对象池？
   * <objectFactory type="org.mybatis.example.ExampleObjectFactory">
   *   <property name="someProperty" value="100"/>
   * </objectFactory>
   */
  private void objectFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties properties = context.getChildrenAsProperties();
      ObjectFactory factory = (ObjectFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(properties);
      configuration.setObjectFactory(factory);
    }
  }

  // 对象包装工厂
  private void objectWrapperFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ObjectWrapperFactory factory = (ObjectWrapperFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setObjectWrapperFactory(factory);
    }
  }

  private void reflectorFactoryElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      ReflectorFactory factory = (ReflectorFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      configuration.setReflectorFactory(factory);
    }
  }

  /**
   *   properties
   *   <properties resource="org/mybatis/example/config.properties">
   *       <property name="username" value="dev_user"/>
   *       <property name="password" value="F2Fa3!33TYyg"/>
   *   </properties>
   */
  private void propertiesElement(XNode context) throws Exception {
    if (context != null) {
      //如果在这些地方,属性多于一个的话,MyBatis 按照如下的顺序加载它们:

      //1.在 properties 元素体内指定的属性首先被读取。
      //2.从类路径下资源或 properties 元素的 url 属性中加载的属性第二被读取,它会覆盖已经存在的完全一样的属性。
      //3.作为方法参数传递的属性最后被读取, 它也会覆盖任一已经存在的完全一样的属性,这些属性可能是从 properties 元素体内和资源/url 属性中加载的。
      //传入方式是调用构造函数时传入，public XMLConfigBuilder(Reader reader, String environment, Properties props)

      //1.XNode.getChildrenAsProperties函数方便得到孩子所有Properties
      Properties defaults = context.getChildrenAsProperties();
      //2.然后查找resource或者url,加入前面的Properties
      String resource = context.getStringAttribute("resource");
      String url = context.getStringAttribute("url");
      if (resource != null && url != null) {
        throw new BuilderException("The properties element cannot specify both a URL and a resource based property file reference.  Please specify one or the other.");
      }
      if (resource != null) {
        defaults.putAll(Resources.getResourceAsProperties(resource));
      } else if (url != null) {
        defaults.putAll(Resources.getUrlAsProperties(url));
      }
      //3.Variables也全部加入Properties
      Properties vars = configuration.getVariables();
      if (vars != null) {
        defaults.putAll(vars);
      }
      parser.setVariables(defaults);
      configuration.setVariables(defaults);
    }
  }

  /**
   * 设置
   * 这些是极其重要的调整, 它们会修改 MyBatis 在运行时的行为方式
   * <settings>
   *   <setting name="cacheEnabled" value="true"/>
   *   <setting name="lazyLoadingEnabled" value="true"/>
   *   <setting name="multipleResultSetsEnabled" value="true"/>
   *   <setting name="useColumnLabel" value="true"/>
   *   <setting name="useGeneratedKeys" value="false"/>
   *   <setting name="enhancementEnabled" value="false"/>
   *   <setting name="defaultExecutorType" value="SIMPLE"/>
   *   <setting name="defaultStatementTimeout" value="25000"/>
   *   <setting name="safeRowBoundsEnabled" value="false"/>
   *   <setting name="mapUnderscoreToCamelCase" value="false"/>
   *   <setting name="localCacheScope" value="SESSION"/>
   *   <setting name="jdbcTypeForNull" value="OTHER"/>
   *   <setting name="lazyLoadTriggerMethods" value="equals,clone,hashCode,toString"/>
   * </settings>
   */
  private void settingsElement(Properties props) {
    // 如何自动映射列到字段/ 属性
    configuration.setAutoMappingBehavior(AutoMappingBehavior.valueOf(props.getProperty("autoMappingBehavior", "PARTIAL")));
    configuration.setAutoMappingUnknownColumnBehavior(AutoMappingUnknownColumnBehavior.valueOf(props.getProperty("autoMappingUnknownColumnBehavior", "NONE")));
    configuration.setCacheEnabled(booleanValueOf(props.getProperty("cacheEnabled"), true));
    // 延迟加载的核心技术就是用代理模式，CGLIB/JAVASSIST两者选一
    configuration.setProxyFactory((ProxyFactory) createInstance(props.getProperty("proxyFactory")));
    // 延迟加载
    configuration.setLazyLoadingEnabled(booleanValueOf(props.getProperty("lazyLoadingEnabled"), false));
    // 延迟加载时，每种属性是否还要按需加载
    configuration.setAggressiveLazyLoading(booleanValueOf(props.getProperty("aggressiveLazyLoading"), false));
    // 允不允许多种结果集从一个单独的语句中返回
    configuration.setMultipleResultSetsEnabled(booleanValueOf(props.getProperty("multipleResultSetsEnabled"), true));
    // 使用列标签代替列名
    configuration.setUseColumnLabel(booleanValueOf(props.getProperty("useColumnLabel"), true));
    // 允许 JDBC 支持生成的键
    configuration.setUseGeneratedKeys(booleanValueOf(props.getProperty("useGeneratedKeys"), false));
    // 配置默认的执行器
    configuration.setDefaultExecutorType(ExecutorType.valueOf(props.getProperty("defaultExecutorType", "SIMPLE")));
    // 超时时间
    configuration.setDefaultStatementTimeout(integerValueOf(props.getProperty("defaultStatementTimeout"), null));
    configuration.setDefaultFetchSize(integerValueOf(props.getProperty("defaultFetchSize"), null));
    configuration.setDefaultResultSetType(resolveResultSetType(props.getProperty("defaultResultSetType")));
    // 是否将DB字段自动映射到驼峰式Java属性（A_COLUMN-->aColumn）
    configuration.setMapUnderscoreToCamelCase(booleanValueOf(props.getProperty("mapUnderscoreToCamelCase"), false));
    // 嵌套语句上使用RowBounds
    configuration.setSafeRowBoundsEnabled(booleanValueOf(props.getProperty("safeRowBoundsEnabled"), false));
    // 默认用session级别的缓存
    configuration.setLocalCacheScope(LocalCacheScope.valueOf(props.getProperty("localCacheScope", "SESSION")));
    // 为null值设置jdbctype
    configuration.setJdbcTypeForNull(JdbcType.valueOf(props.getProperty("jdbcTypeForNull", "OTHER")));
    // Object的哪些方法将触发延迟加载
    configuration.setLazyLoadTriggerMethods(stringSetValueOf(props.getProperty("lazyLoadTriggerMethods"), "equals,clone,hashCode,toString"));
    // 使用安全的ResultHandler
    configuration.setSafeResultHandlerEnabled(booleanValueOf(props.getProperty("safeResultHandlerEnabled"), true));
    // 动态SQL生成语言所使用的脚本语言
    configuration.setDefaultScriptingLanguage(resolveClass(props.getProperty("defaultScriptingLanguage")));
    configuration.setDefaultEnumTypeHandler(resolveClass(props.getProperty("defaultEnumTypeHandler")));
    // 当结果集中含有Null值时是否执行映射对象的setter或者Map对象的put方法。此设置对于原始类型如int,boolean等无效。
    configuration.setCallSettersOnNulls(booleanValueOf(props.getProperty("callSettersOnNulls"), false));
    configuration.setUseActualParamName(booleanValueOf(props.getProperty("useActualParamName"), true));
    configuration.setReturnInstanceForEmptyRow(booleanValueOf(props.getProperty("returnInstanceForEmptyRow"), false));
    // logger名字的前缀
    configuration.setLogPrefix(props.getProperty("logPrefix"));
    // 配置工厂
    configuration.setConfigurationFactory(resolveClass(props.getProperty("configurationFactory")));
    configuration.setShrinkWhitespacesInSql(booleanValueOf(props.getProperty("shrinkWhitespacesInSql"), false));
    configuration.setDefaultSqlProviderType(resolveClass(props.getProperty("defaultSqlProviderType")));
    configuration.setNullableOnForEach(booleanValueOf(props.getProperty("nullableOnForEach"), false));
  }

  /**
   * 	环境
   * 	<environments default="development">
   * 	  <environment id="development">
   * 	    <transactionManager type="JDBC">
   * 	      <property name="..." value="..."/>
   * 	    </transactionManager>
   * 	    <dataSource type="POOLED">
   * 	      <property name="driver" value="${driver}"/>
   * 	      <property name="url" value="${url}"/>
   * 	      <property name="username" value="${username}"/>
   * 	      <property name="password" value="${password}"/>
   * 	    </dataSource>
   * 	  </environment>
   * 	</environments>
   */
  private void environmentsElement(XNode context) throws Exception {
    if (context != null) {
      if (environment == null) {
        environment = context.getStringAttribute("default");
      }
      for (XNode child : context.getChildren()) {
        String id = child.getStringAttribute("id");
        // 循环比较id是否就是指定的environment
        if (isSpecifiedEnvironment(id)) {
          // 事务管理器
          TransactionFactory txFactory = transactionManagerElement(child.evalNode("transactionManager"));
          // 数据源
          DataSourceFactory dsFactory = dataSourceElement(child.evalNode("dataSource"));
          DataSource dataSource = dsFactory.getDataSource();
          Environment.Builder environmentBuilder = new Environment.Builder(id)
              .transactionFactory(txFactory)
              .dataSource(dataSource);
          configuration.setEnvironment(environmentBuilder.build());
          break;
        }
      }
    }
  }

  /**
   * 	8.databaseIdProvider
   * 	可以根据不同数据库执行不同的SQL，sql要加databaseId属性
   * 	这个功能感觉不是很实用，真要多数据库支持，那SQL工作量将会成倍增长，用mybatis以后一般就绑死在一个数据库上了。但也是一个不得已的方法吧
   *  可以参考org.apache.ibatis.submitted.multidb包里的测试用例
   * 	<databaseIdProvider type="VENDOR">
   * 	  <property name="SQL Server" value="sqlserver"/>
   * 	  <property name="DB2" value="db2"/>
   * 	  <property name="Oracle" value="oracle" />
   * 	</databaseIdProvider>
   */
  private void databaseIdProviderElement(XNode context) throws Exception {
    DatabaseIdProvider databaseIdProvider = null;
    if (context != null) {
      String type = context.getStringAttribute("type");
      // awful patch to keep backward compatibility
      // 与老版本兼容
      if ("VENDOR".equals(type)) {
        type = "DB_VENDOR";
      }
      Properties properties = context.getChildrenAsProperties();
      databaseIdProvider = (DatabaseIdProvider) resolveClass(type).getDeclaredConstructor().newInstance();
      databaseIdProvider.setProperties(properties);
    }
    Environment environment = configuration.getEnvironment();
    if (environment != null && databaseIdProvider != null) {
      // 得到当前的databaseId，可以调用DatabaseMetaData.getDatabaseProductName()得到诸如"Oracle (DataDirect)"的字符串，
      // 然后和预定义的property比较,得出目前究竟用的是什么数据库
      String databaseId = databaseIdProvider.getDatabaseId(environment.getDataSource());
      configuration.setDatabaseId(databaseId);
    }
  }

  /**
   * 事务管理器
   * <transactionManager type="JDBC">
   *   <property name="..." value="..."/>
   * </transactionManager>
   */
  private TransactionFactory transactionManagerElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      TransactionFactory factory = (TransactionFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a TransactionFactory.");
  }

  /**
   * 数据源
   * <dataSource type="POOLED">
   *   <property name="driver" value="${driver}"/>
   *   <property name="url" value="${url}"/>
   *   <property name="username" value="${username}"/>
   *   <property name="password" value="${password}"/>
   * </dataSource>
   */
  private DataSourceFactory dataSourceElement(XNode context) throws Exception {
    if (context != null) {
      String type = context.getStringAttribute("type");
      Properties props = context.getChildrenAsProperties();
      DataSourceFactory factory = (DataSourceFactory) resolveClass(type).getDeclaredConstructor().newInstance();
      factory.setProperties(props);
      return factory;
    }
    throw new BuilderException("Environment declaration requires a DataSourceFactory.");
  }

  /**
   * 	类型处理器
   * 	<typeHandlers>
   * 	  <typeHandler handler="org.mybatis.example.ExampleTypeHandler"/>
   * 	</typeHandlers>
   * or
   * 	<typeHandlers>
   * 	  <package name="org.mybatis.example"/>
   * 	</typeHandlers>
   */
  private void typeHandlerElement(XNode parent) {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        // 如果是package
        if ("package".equals(child.getName())) {
          String typeHandlerPackage = child.getStringAttribute("name");
          //（一）调用TypeHandlerRegistry.register，去包下找所有类
          typeHandlerRegistry.register(typeHandlerPackage);
        } else {
          // 如果是typeHandler
          String javaTypeName = child.getStringAttribute("javaType");
          String jdbcTypeName = child.getStringAttribute("jdbcType");
          String handlerTypeName = child.getStringAttribute("handler");
          Class<?> javaTypeClass = resolveClass(javaTypeName);
          JdbcType jdbcType = resolveJdbcType(jdbcTypeName);
          //（二）调用TypeHandlerRegistry.register(以下是3种不同的参数形式)
          Class<?> typeHandlerClass = resolveClass(handlerTypeName);
          if (javaTypeClass != null) {
            if (jdbcType == null) {
              typeHandlerRegistry.register(javaTypeClass, typeHandlerClass);
            } else {
              typeHandlerRegistry.register(javaTypeClass, jdbcType, typeHandlerClass);
            }
          } else {
            typeHandlerRegistry.register(typeHandlerClass);
          }
        }
      }
    }
  }

  /**
   * 	映射器
   * 	1使用类路径
   * 	<mappers>
   * 	  <mapper resource="org/mybatis/builder/AuthorMapper.xml"/>
   * 	  <mapper resource="org/mybatis/builder/BlogMapper.xml"/>
   * 	  <mapper resource="org/mybatis/builder/PostMapper.xml"/>
   * 	</mappers>
   *
   * 	2使用绝对url路径
   * 	<mappers>
   * 	  <mapper url="file:///var/mappers/AuthorMapper.xml"/>
   * 	  <mapper url="file:///var/mappers/BlogMapper.xml"/>
   * 	  <mapper url="file:///var/mappers/PostMapper.xml"/>
   * 	</mappers>
   *
   * 	3使用java类名
   * 	<mappers>
   * 	  <mapper class="org.mybatis.builder.AuthorMapper"/>
   * 	  <mapper class="org.mybatis.builder.BlogMapper"/>
   * 	  <mapper class="org.mybatis.builder.PostMapper"/>
   * 	</mappers>
   *
   * 	4自动扫描包下所有映射器
   * 	<mappers>
   * 	  <package name="org.mybatis.builder"/>
   * 	</mappers>
   */
  private void mapperElement(XNode parent) throws Exception {
    if (parent != null) {
      for (XNode child : parent.getChildren()) {
        if ("package".equals(child.getName())) {
          // 4自动扫描包下所有映射器
          String mapperPackage = child.getStringAttribute("name");
          configuration.addMappers(mapperPackage);
        } else {
          String resource = child.getStringAttribute("resource");
          String url = child.getStringAttribute("url");
          String mapperClass = child.getStringAttribute("class");
          if (resource != null && url == null && mapperClass == null) {
            // 1使用类路径
            ErrorContext.instance().resource(resource);
            try(InputStream inputStream = Resources.getResourceAsStream(resource)) {
              // 映射器比较复杂，调用XMLMapperBuilder
              // 注意在for循环里每个mapper都重新new一个XMLMapperBuilder，来解析
              XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, resource, configuration.getSqlFragments());
              mapperParser.parse();
            }
          } else if (resource == null && url != null && mapperClass == null) {
            // 2使用绝对url路径
            ErrorContext.instance().resource(url);
            try(InputStream inputStream = Resources.getUrlAsStream(url)){
              // 映射器比较复杂，调用XMLMapperBuilder
              XMLMapperBuilder mapperParser = new XMLMapperBuilder(inputStream, configuration, url, configuration.getSqlFragments());
              mapperParser.parse();
            }
          } else if (resource == null && url == null && mapperClass != null) {
            // 3使用java类名
            Class<?> mapperInterface = Resources.classForName(mapperClass);
            // 直接把这个映射加入配置
            configuration.addMapper(mapperInterface);
          } else {
            throw new BuilderException("A mapper element may only specify a url, resource or class, but not more than one.");
          }
        }
      }
    }
  }

  // 比较id和environment是否相等
  private boolean isSpecifiedEnvironment(String id) {
    if (environment == null) {
      throw new BuilderException("No environment specified.");
    }
    if (id == null) {
      throw new BuilderException("Environment requires an id attribute.");
    }
    return environment.equals(id);
  }

}
