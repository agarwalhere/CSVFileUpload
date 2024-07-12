/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSP_Servlet/Servlet.java to edit this template
 */
package com.myapp;

import java.io.IOException;
import jakarta.servlet.ServletException;
import jakarta.servlet.annotation.MultipartConfig;
import jakarta.servlet.annotation.WebServlet;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.Part;
import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

@WebServlet("/upload")
@MultipartConfig
public class UploadServlet extends HttpServlet {
private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = Logger.getLogger(UploadServlet.class.getName());

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        
    }

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {
        LOGGER.log(Level.INFO, "Received file upload request");
        Part filePart = request.getPart("file");
        if (filePart == null) {
            LOGGER.log(Level.SEVERE, "No file part in the request");
            response.getWriter().write("File part is missing");
            return;
        }

        String fileName = filePart.getSubmittedFileName();
        File file = new File("/Users/shourey/Documents/ihat/" + fileName);
        try (InputStream inputStream = filePart.getInputStream()) {
            Files.copy(inputStream, file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            LOGGER.log(Level.INFO, "File saved successfully: {0}", file.getAbsolutePath());
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to save file", e);
            response.getWriter().write("Failed to save file");
            return;
        }

        try {
            processExcelFile(file.getAbsolutePath());
            LOGGER.log(Level.INFO, "File processed successfully");
            response.getWriter().write("File uploaded and processed successfully");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to process file", e);
            response.getWriter().write("Failed to process file");
        }
    }
    private void processExcelFile(String filePath) {
        ExcelProcessing.processExcelFile(filePath);
    }

    @Override
    public String getServletInfo() {
        return "Short description";
    }

}
