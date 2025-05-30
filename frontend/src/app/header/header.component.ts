import { Component } from '@angular/core';
import { Router } from '@angular/router';

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.css']
})
export class HeaderComponent {
  constructor(private router: Router) {}

  downloadFiles(): void {
    const files = [
      { url: 'assets/Regles.pdf', name: 'rules.pdf' },
      { url: 'assets/Dictionnaire_de_donnees.xlsx', name: 'dictionnaire.xlsx' }
    ];
  
    files.forEach(file => {
      const link = document.createElement('a');
      link.href = file.url;
      link.download = file.name;
      document.body.appendChild(link);
      link.click();
      document.body.removeChild(link);
    });
  }
}
