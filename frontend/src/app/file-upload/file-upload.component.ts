import { Component, Input, Output, EventEmitter } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-file-upload',
  templateUrl: './file-upload.component.html',
  styleUrls: ['./file-upload.component.css'],
})
export class FileUploadComponent {
  choice: string | null = null;
  email: string = '';
  message: string = '';
  file: File | null = null;
  uploadMessage: string = '';
  isLoading: boolean = false;

  constructor(private router: Router) {
    this.choice = localStorage.getItem('projectChoice');
  }

  onDrop(event: DragEvent) {
    event.preventDefault();
    const files = event.dataTransfer?.files;
    if (files && files.length > 0) {
      this.handleFile(files[0]);
    }
  }

  onDragOver(event: DragEvent) {
    event.preventDefault();
  }

  onDragLeave(event: DragEvent) {
    event.preventDefault();
  }

  onFileChange(event: Event) {
    const input = event.target as HTMLInputElement;
    if (input.files && input.files.length > 0) {
      this.handleFile(input.files[0]);
    }
  }

  handleFile(file: File) {
    if (file.type === 'application/zip' || file.name.endsWith('.zip')) {
      this.file = file;
      this.uploadMessage = `Selected file: ${file.name}`;
    } else {
      alert('Please upload a ZIP file.');
    }
  }

  analyze() {
    if (this.file && this.choice) {
      this.isLoading = true;
      const formData = new FormData();
      formData.append('file', this.file);
      formData.append('email', this.email);
      formData.append('message', this.message);
      formData.append('choice', this.choice);

      fetch('http://localhost:5000/upload', {
        method: 'POST',
        body: formData,
      })
        .then((response) => {
          if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
          }
          return response.json();
        })
        .then((data) => {
          console.log(data);
          this.uploadMessage = 'File uploaded and analyzed successfully!';
          this.isLoading = false;
          window.location.href =
            'http://localhost:3000/d/bc71b594-d3c7-42d6-acfb-ea226470477f/new-dashboard?orgId=1&from=now-6h&to=now&timezone=browser&showCategory=Repeat%20options';
        })
        .catch((error) => {
          console.error('Error:', error);
          this.uploadMessage = 'Error uploading file. ' + error.message;
          this.isLoading = false;
        });
    } else {
      alert('Please select a choice and drop a ZIP file first.');
    }
  }

  cancel() {
    this.file = null;
    this.uploadMessage = '';
    this.isLoading = false;
    console.log('Upload canceled.');
  }
}
