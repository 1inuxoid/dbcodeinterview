package app;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
public class DatabaseControllerTests {

    public static final MediaType APPLICATION_JSON_UTF8 = new MediaType(MediaType.APPLICATION_JSON.getType(),
            MediaType.APPLICATION_JSON.getSubtype(), Charset.forName("utf8"));


    @Autowired
    private MockMvc mockMvc;
    private ObjectWriter ow;

    @Before
    public void init() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(SerializationFeature.WRAP_ROOT_VALUE, false);
        ow = mapper.writer().withDefaultPrettyPrinter();
        File dbFolder = new File(DatabaseService.DB_FOLDER);

        // deleting all test files for re-runnability
        File[] dbFiles = dbFolder.listFiles();
        if (dbFiles != null) {
            for (File file : dbFiles) {
                if (file.getName().startsWith("test")) {
                    file.delete();
                }
            }
        }
    }

    @Test
    public void testSimpleCRUD() throws Exception {
        int i = 1;
        this.mockMvc.perform(post("/insert/testTable")
                .contentType(APPLICATION_JSON_UTF8)
                .content(ow.writeValueAsString(Arrays.asList("value" + i++, "value" + i++))))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("0")));

        this.mockMvc.perform(post("/insert/testTable")
                .contentType(APPLICATION_JSON_UTF8)
                .content(ow.writeValueAsString(Arrays.asList("value" + i++, "value" + i++))))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("1")));


        this.mockMvc.perform(get("/select/testTable/1")
                .contentType(APPLICATION_JSON_UTF8))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("[\"1\",\"value3\",\"value4\"]")));

        this.mockMvc.perform(get("/select/testTable/0")
                .contentType(APPLICATION_JSON_UTF8))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("[\"0\",\"value1\",\"value2\"]")));

        this.mockMvc.perform(post("/update/testTable/1")
                .contentType(APPLICATION_JSON_UTF8)
                .content(ow.writeValueAsString(Arrays.asList("value10", "value11"))))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("true")));

        this.mockMvc.perform(get("/select/testTable/1")
                .contentType(APPLICATION_JSON_UTF8))
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(content().string(equalTo("[\"1\",\"value10\",\"value11\"]")));
    }
}