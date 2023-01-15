package org.apache.flume.source.clickhousesource.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@ToString
@EqualsAndHashCode
public class MonitoringActivity implements Serializable {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    private final Long id;
    private final LocalDateTime time;
    private final Long employeeAccountId;
    private final Byte timeOffset;
    private final String program;
    private final String version;
    private final String executablePath;
    private final Short type;
    private final Byte inputType;
    private final String mainWindow;
    private final String tab;
    private final String url;
    private final String filePath;
    private final List<String> uiHierarchyName;
    private final List<Integer> uiHierarchyCtrl;
    private final List<String> parametersName;
    private final List<String> parametersValue;
    private final Integer cpuLoading;
    private final Integer memoryLoading;
    private final String computerName;

    public MonitoringActivity(ResultSet resultSet) throws SQLException {
        id = resultSet.getLong("id");
        this.time = LocalDateTime.parse(resultSet.getString("time"), formatter);
        this.cpuLoading = resultSet.getInt("cpu_loading");
        this.memoryLoading = resultSet.getInt("memory_loading");
        this.employeeAccountId = resultSet.getLong("employee_account_id");
        this.timeOffset = resultSet.getByte("time_offset");
        this.program = resultSet.getString("program");
        this.version = resultSet.getString("version");
        this.executablePath = resultSet.getString("executable_path");
        this.type = resultSet.getShort("type");
        this.inputType = resultSet.getByte("input_type");
        this.mainWindow = resultSet.getString("main_window");
        this.tab = resultSet.getString("tab");
        this.url = resultSet.getString("url");
        this.filePath = resultSet.getString("file_path");
        uiHierarchyName = Arrays.stream((String[]) resultSet.getArray("ui_hierarchy_name").getArray()).collect(Collectors.toList());
        uiHierarchyCtrl = Arrays.stream((int[]) resultSet.getArray("ui_hierarchy_ctrl").getArray()).boxed().collect(Collectors.toList());
        parametersName = Arrays.stream((String[]) resultSet.getArray("parameters_name").getArray()).collect(Collectors.toList());
        parametersValue = Arrays.stream((String[]) resultSet.getArray("parameters_value").getArray()).collect(Collectors.toList());
        this.computerName = resultSet.getString("computer_name");
    }
}
