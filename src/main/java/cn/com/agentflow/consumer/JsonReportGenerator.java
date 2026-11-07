package cn.com.agentflow.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;

/**
 * 报表数据产物生成器：把任务参数与生成时间固化为 JSON 快照。
 * 先写临时文件再覆盖替换，同一 taskId 重复消费只会覆盖同一路径，不产生多份产物；
 * pdf/docx/xlsx 的最终渲染由下游渲染服务消费该快照完成，本仓库不引入额外渲染依赖。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class JsonReportGenerator implements ReportGenerator {

    private final ObjectMapper mapper;
    private final ReportTaskProperties properties;

    @Override
    public void generate(ReportTaskMessage message) throws IOException {
        ObjectNode report = mapper.createObjectNode();
        report.put("taskId", message.taskId());
        report.put("format", message.format());
        report.set("assetIds", message.assetIds());
        report.put("assetCount", message.assetIds().size());
        report.put("generatedAt", OffsetDateTime.now().toString());

        Path dir = Path.of(properties.getOutputDir()).toAbsolutePath().normalize();
        Files.createDirectories(dir);
        Path target = dir.resolve(message.taskId() + ".json");
        Path temp = dir.resolve(message.taskId() + ".json.tmp");
        mapper.writerWithDefaultPrettyPrinter().writeValue(temp.toFile(), report);
        Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING);
        log.info("报表数据产物已生成: taskId={} path={}", message.taskId(), target);
    }
}
