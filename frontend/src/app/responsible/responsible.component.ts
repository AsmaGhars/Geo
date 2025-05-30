import { HttpClient } from '@angular/common/http';
import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-responsible',
  templateUrl: './responsible.component.html',
  styleUrls: ['./responsible.component.css']
})
export class ResponsibleComponent {
  email: string = '';
  errorMessage: string = ''; 

  constructor(private http: HttpClient, private router: Router) {}

  validateInputs() {
    this.errorMessage = ''; 

    const emailPattern = /^[^\s@]+@[^\s@]+\.[^\s@]+$/; 
    if (this.email.trim() === '') {
      this.errorMessage = 'Please enter an email.';
    } else if (!emailPattern.test(this.email)) {
      this.errorMessage = 'Please enter a valid email address.';
    } else {
      const payload = {
        email: this.email,
      };

      this.http.post('http://localhost:5000/save-email', payload).subscribe(
        response => {
          console.log('Email envoyé au backend:', response);
          this.router.navigate(['/file-upload']);
        },
        error => {
          console.error('Erreur:', error);
        }
      );
    }
  }
}