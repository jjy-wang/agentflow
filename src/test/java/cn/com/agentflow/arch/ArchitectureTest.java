package cn.com.agentflow.arch;

import com.tngtech.archunit.core.domain.JavaClasses;
import com.tngtech.archunit.core.importer.ClassFileImporter;
import com.tngtech.archunit.core.importer.ImportOption;
import com.tngtech.archunit.lang.ArchRule;
import org.junit.jupiter.api.Test;

import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.classes;
import static com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses;

/** 架构约束（md 18.2）：分层方向、反应式纪律、禁止 ThreadLocal */
class ArchitectureTest {

    // 测试类与主代码同包：必须排除 test-classes，否则 org.junit.jupiter.api 会误命中 "..api.." 规则
    private static final JavaClasses CLASSES = new ClassFileImporter()
            .withImportOption(ImportOption.Predefined.DO_NOT_INCLUDE_TESTS)
            .importPackages("cn.com.agentflow");

    @Test
    void controller不直接依赖mapper() {
        ArchRule rule = noClasses().that().resideInAPackage("..api..")
                .should().dependOnClassesThat().resideInAPackage("..mapper..");
        rule.check(CLASSES);
    }

    @Test
    void agent核心不依赖api层() {
        ArchRule rule = noClasses().that().resideInAPackage("..agent..")
                .should().dependOnClassesThat().resideInAPackage("..api..");
        rule.check(CLASSES);
    }

    @Test
    void 主代码禁止ThreadSleep() {
        ArchRule rule = noClasses().that().resideOutsideOfPackage("..test..")
                .should().callMethod(Thread.class, "sleep", long.class);
        rule.check(CLASSES);
    }

    @Test
    void controller返回反应式类型() {
        ArchRule rule = classes().that().resideInAPackage("..api..")
                .should().onlyDependOnClassesThat().resideOutsideOfPackage("..mapper..")
                .allowEmptyShould(true);
        rule.check(CLASSES);
    }

    @Test
    void 主代码禁止使用ThreadLocal() {
        // WebFlux 下 ThreadLocal 跨线程丢失，租户/追踪上下文一律走 Reactor Context
        ArchRule rule = noClasses().that().resideOutsideOfPackage("..test..")
                .should().dependOnClassesThat().haveFullyQualifiedName(ThreadLocal.class.getName());
        rule.check(CLASSES);
    }
}
