import { Component } from '@angular/core';
import emailjs from '@emailjs/browser';

@Component({
  selector: 'app-contact',
  templateUrl: './contact.component.html',
  styleUrls: ['./contact.component.css']
})
export class ContactComponent {
  name: string = '';
  email: string = '';
  message: string = '';
  submitted: boolean = false;
  isLoading: boolean = false;
  errorMessage: string = '';

  constructor() {
    emailjs.init('zz-Ix7_kpUX2urj63');
  }

  async sendEmail() {
    if (!this.name || !this.email || !this.message) {
      this.errorMessage = 'Please fill in all required fields';
      return;
    }

    this.isLoading = true;
    this.errorMessage = '';

    try {
      const templateParams = {
        name: this.name,
        email: this.email,
        message: this.message,
        subject: 'Contact Us'
      };

      await emailjs.send(
        'service_imaj484',
        'template_qbq2fgf',
        templateParams
      );

      this.submitted = true;
    } catch (error) {
      console.error('Email sending failed:', error);
      this.errorMessage = 'Failed to send your message. Please try again later.';
    } finally {
      this.isLoading = false;
    }
  }
}

